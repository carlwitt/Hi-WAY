package de.huberlin.wbi.hiway.am;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import de.huberlin.wbi.hiway.common.HiWayInvocation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * Receives messages about allocated and completed containers, node updates, reports workflow execution progress to the web interface.
 */
class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private final WorkflowDriver am;
	/** a data structure storing the invocation launched by each container **/
	private final Map<ContainerId, HiWayInvocation> containerIdToInvocation = new HashMap<>();

	/** a queue for allocated containers that have yet to be assigned a task **/
	private final Queue<Container> containerQueue = new LinkedList<>();

	public RMCallbackHandler(WorkflowDriver am) {
		super();
		this.am = am;
	}

	@SuppressWarnings("unchecked")
	private ContainerRequest findFirstMatchingRequest(Container container) {
		List<? extends Collection<ContainerRequest>> requestCollections = am.getScheduler().relaxLocality() ? am.getAmRMClient().getMatchingRequests(
				container.getPriority(), ResourceRequest.ANY, container.getResource()) : am.getAmRMClient().getMatchingRequests(container.getPriority(),
				container.getNodeId().getHost(), container.getResource());

		for (Collection<ContainerRequest> requestCollection : requestCollections) {
			ContainerRequest request = requestCollection.iterator().next();
			if (request.getCapability().equals(container.getResource()))
				return request;
		}
		return null;
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		if (am.getScheduler() == null) return 0f;
		int totalTasks = am.getScheduler().getNumberOfTotalTasks();
		return (totalTasks == 0) ? 0 : (float) am.getNumCompletedContainers().get() / totalTasks;
	}

	private void launchTask(TaskInstance task, Container allocatedContainer) {
		containerIdToInvocation.put(allocatedContainer.getId(), new HiWayInvocation(task));
		am.taskInstanceByContainer.putIfAbsent(allocatedContainer, task);

		LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, am.getContainerListener(), task, am);
		Thread launchThread = new Thread(runnableLaunchContainer);

		/* launch and start the container on a separate thread to keep the main thread unblocked as all containers may not be allocated at one go. */
		am.getLaunchThreads().add(launchThread);
		launchThread.start();
	}

	private void launchTasks() {
		while (!containerQueue.isEmpty() && !am.getScheduler().nothingToSchedule()) {
			Container allocatedContainer = containerQueue.remove();

			long tic = System.currentTimeMillis();
			TaskInstance task = am.getScheduler().getTask(allocatedContainer);
			long toc = System.currentTimeMillis();

			if (task.getTries() == 1) {
				JSONObject obj = new JSONObject();
				try {
					obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
				} catch (JSONException e) {
					onError(e);
				}
				task.getReport().add(
						new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
								null, HiwayDBI.KEY_INVOC_TIME_SCHED, obj));
				task.getReport().add(
						new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
								null, HiwayDBI.KEY_INVOC_HOST, allocatedContainer.getNodeId().getHost()));
			}
			launchTask(task, allocatedContainer);
		}
	}

	/**
	 * Adds the newly arrived containers to this object's {@link RMCallbackHandler#containerQueue}.
	 * Removes satisfied container requests from the AMs AMRMClient, releases containers for which no request was found.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		if (HiWayConfiguration.verbose) {
			for (Container container : allocatedContainers) {
				WorkflowDriver.writeToStdout("Allocated container " + container.getId().getContainerId() + " on node " + container.getNodeId().getHost()
						+ " with capability " + container.getResource().getVirtualCores() + ":" + container.getResource().getMemory());
			}
		}

		for (Container container : allocatedContainers) {
			/* log */ logHiwayEventContainerAllocated(container);

			ContainerRequest request = findFirstMatchingRequest(container);

			if (request != null) {
				if (HiWayConfiguration.verbose)
					WorkflowDriver.writeToStdout("Removing container request " + request.getNodes() + ":" + request.getCapability().getVirtualCores() + ":"
							+ request.getCapability().getMemory());
				am.getAmRMClient().removeContainerRequest(request);
				am.getNumAllocatedContainers().incrementAndGet();

				// add to ready containers
				containerQueue.add(container);

			} else {
				if (HiWayConfiguration.verbose)
					WorkflowDriver.writeToStdout("Releasing container due to no matching request found. ID " + container.getId().getContainerId() + " on node " + container.getNodeId().getHost()
							+ " with capability " + container.getResource().getVirtualCores() + ":" + container.getResource().getMemory());
				am.getAmRMClient().releaseAssignedContainer(container.getId());
			}
		}

		launchTasks();
	}

	/**
	 * Reads the diagnostics string of the container and logs accordingly (e.g., force killed by YARN).
	 * If the status is ok, looks up the invocation launched in the container and signals task success to the AM.
	 * Then calls {@link RMCallbackHandler#launchTasks()}.
	 */
	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		for (ContainerStatus containerStatus : completedContainers) {

			/* log */ logHiwayEventContainerCompleted(containerStatus);

			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);

			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			String diagnostics = containerStatus.getDiagnostics();
			ContainerId containerId = containerStatus.getContainerId();

			// The container was released by the framework (e.g., it was a speculative copy of a finished task)
			if (diagnostics.equals(SchedulerUtils.RELEASED_CONTAINER)) {
				WorkflowDriver.writeToStdout("Container was released." + ", containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
						+ ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			} else if (exitStatus == ExitCode.FORCE_KILLED.getExitCode()) {
				WorkflowDriver.writeToStdout("Container was force killed." + ", containerID=" + containerStatus.getContainerId() + ", state="
						+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			} else if (containerIdToInvocation.containsKey(containerId)) {

				HiWayInvocation invocation = containerIdToInvocation.get(containerStatus.getContainerId());
				TaskInstance finishedTask = invocation.task;

				if (exitStatus == 0) {
					// this task might have been completed previously (e.g., via speculative replication)
					if (!finishedTask.isCompleted()) {
						finishedTask.setCompleted();

						am.evaluateReport(finishedTask, containerId);

						for (JsonReportEntry entry : finishedTask.getReport()) {
							am.writeEntryToLog(entry);
						}

						long runtime = System.currentTimeMillis() - invocation.timestamp;
						JSONObject obj = new JSONObject();
						try {
							obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(runtime));
						} catch (JSONException e) {
							e.printStackTrace(System.out);
							System.exit(-1);
						}
						am.writeEntryToLog(new JsonReportEntry(System.currentTimeMillis(), finishedTask.getWorkflowId(), finishedTask.getTaskId(), finishedTask
								.getTaskName(), finishedTask.getLanguageLabel(), finishedTask.getId(), null, JsonReportEntry.KEY_INVOC_TIME, obj));

						Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskCompleted(finishedTask, containerStatus, runtime);
						for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
							WorkflowDriver.writeToStdout("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
							am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
							am.getNumKilledContainers().incrementAndGet();
						}

						am.getNumCompletedContainers().incrementAndGet();

						am.taskSuccess(finishedTask, containerId);
					}
				}

				// The container failed horribly.
				else {

					am.taskFailure(finishedTask, containerId);
					am.getNumFailedContainers().incrementAndGet();

					if (exitStatus == ExitCode.TERMINATED.getExitCode()) {
						WorkflowDriver.writeToStdout("Container was terminated." + ", containerID=" + containerStatus.getContainerId() + ", state="
								+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
								+ containerStatus.getDiagnostics());
					} else {
						WorkflowDriver.writeToStdout("Container completed with failure." + ", containerID=" + containerStatus.getContainerId() + ", state="
								+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
								+ containerStatus.getDiagnostics());

						Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskFailed(finishedTask, containerStatus);
						for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
							WorkflowDriver.writeToStdout("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
							am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
							am.getNumKilledContainers().incrementAndGet();
						}
					}
				}
			}

			/* The container was aborted by the framework without it having been assigned an invocation (e.g., because the RM allocated more containers than
			 * requested) */
			else {
				WorkflowDriver.writeToStdout("Container failed." + ", containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
						+ ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			}
		}

		launchTasks();
	}

	@Override
	public void onError(Throwable e) {
		e.printStackTrace(System.out);
		System.exit(-1);
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
	}

	@Override
	public void onShutdownRequest() {
		WorkflowDriver.writeToStdout("Shutdown Request.");
		am.setDone();
	}

	/** Write a JSON log entry informing about a successfully allocated container.
	 * Called from {@link #onContainersAllocated(List)}.*/
	private void logHiwayEventContainerAllocated(Container container) {
		JSONObject value = new JSONObject();
		try {
			value.put("type", "container-allocated");
			value.put("container-id", container.getId());
			value.put("node-id", container.getNodeId());
			value.put("node-http", container.getNodeHttpAddress());
			value.put("memory", container.getResource().getMemory());
			value.put("vcores", container.getResource().getVirtualCores());
			value.put("service", container.getContainerToken().getService());
		} catch (JSONException e) {
			onError(e);
		}
		am.writeEntryToLog(new JsonReportEntry(am.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
	}

	/**
	 * Write a JSON log entry informing about completed containers.
	 * @param containerStatus the ContainerStatus of as reported back to the {@link #onContainersCompleted(List)} callback.
	 */
	private void logHiwayEventContainerCompleted(ContainerStatus containerStatus) {
		JSONObject value = new JSONObject();
		try {
			value.put("type", "container-completed");
			value.put("container-id", containerStatus.getContainerId());
			value.put("state", containerStatus.getState());
			value.put("exit-code", containerStatus.getExitStatus());
			value.put("diagnostics", containerStatus.getDiagnostics());
		} catch (JSONException e) {
			onError(e);
		}
		am.writeEntryToLog(new JsonReportEntry(am.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
	}
}
