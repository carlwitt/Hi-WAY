package de.huberlin.wbi.hiway.am;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;
import de.huberlin.wbi.hiway.common.HiWayInvocation;
import de.huberlin.wbi.hiway.monitoring.TaskResourceConsumption;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
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
public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private final WorkflowDriver am;
	/** a data structure storing the invocation launched by each container **/
	private final Map<ContainerId, HiWayInvocation> containerIdToInvocation = new HashMap<>();
	/** Keep a reference to the runnable that started the container to stop it polling cadvisor stats. */
	private final Map<ContainerId, LaunchContainerRunnable> containerIdToRunnable = new ConcurrentHashMap<>();

	/** a queue for allocated containers that have yet to be assigned a task **/
	private final Queue<Container> containerQueue = new LinkedList<>();

	public RMCallbackHandler(WorkflowDriver am) {
		super();
		this.am = am;
	}

	/**
	 * Reads the diagnostics string of the container and logs accordingly (e.g., force killed by YARN).
	 * If the status is ok, looks up the invocation launched in the container and signals task success to the AM.
	 * Then calls {@link RMCallbackHandler#launchTasks()}.
	 */
	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {

		for (ContainerStatus containerStatus : completedContainers) {

			// extract status information
			int exitStatus = containerStatus.getExitStatus();
			String diagnostics = containerStatus.getDiagnostics();
			ContainerId containerId = containerStatus.getContainerId();

			// stop monitoring, evaluate results and log them out
			LaunchContainerRunnable runnable = containerIdToRunnable.remove(containerId);
			//TODO find out why there were nullpointerexceptions
			if (runnable!=null && runnable.getMonitor()!=null)runnable.getMonitor().stopMonitoring();
			/* log */ logHiwayEventContainerCompleted(containerStatus, runnable);

			// The container was released by the framework (e.g., it was a speculative copy of a finished task)
			if (diagnostics.equals(SchedulerUtils.RELEASED_CONTAINER)) {
				/* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Container was released. containerID=%s, state=%s, exitStatus=%d, diagnostics=%s", containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(), containerStatus.getDiagnostics()));
			} else if (exitStatus == ExitCode.FORCE_KILLED.getExitCode()) {
				/* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Container was force killed. containerID=%s, state=%s, exitStatus=%d, diagnostics=%s", containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(), containerStatus.getDiagnostics()));
			} else if (containerIdToInvocation.containsKey(containerId)) {
				finalizeRequestedContainer(containerStatus, exitStatus, containerId);
			}
			// container was aborted by the framework without it having been assigned a task (e.g., because the RM allocated more containers than requested)
			else {
				/* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Container failed., containerID=%s, state=%s, exitStatus=%d, diagnostics=%s", containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(), containerStatus.getDiagnostics()));
			}
		}

		launchTasks();
	}

	/**
	 * Adds the newly arrived containers to this object's {@link RMCallbackHandler#containerQueue}.
	 * Removes satisfied container requests from the AMs AMRMClient, releases containers for which no request was found.
	 * Then calls {@link RMCallbackHandler#launchTasks()}.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {

		/* log */ if (HiWayConfiguration.verbose) for (Container container : allocatedContainers) WorkflowDriver.Logger.writeToStdout(String.format("Allocated container %d on node %s with capability %d:%d", container.getId().getContainerId(), container.getNodeId().getHost(), container.getResource().getVirtualCores(), container.getResource().getMemory()));


		for (Container container : allocatedContainers) {

			/* log */ logHiwayEventContainerAllocated(container);

			/*
			 * Matches one of the AmRMClients requests to the given container.
			 * The request needs to have capabilities and priority equal to the given container.
			 * If the scheduler has getRelaxLocality on, the given container may be located on any node, otherwise, it needs to be on the node specified in the request.
			 * @param container A container as received from the YARN Resource Manager.
			 * @return One of the requests in the AmRMClient.
			 */
			ContainerRequest request = null;
			// get requests for all nodes or grouped by node
			List<? extends Collection<ContainerRequest>> requestCollections = am.getScheduler().getRelaxLocality() ?
					am.getAmRMClient().getMatchingRequests(container.getPriority(), ResourceRequest.ANY, container.getResource()) :
					am.getAmRMClient().getMatchingRequests(container.getPriority(), container.getNodeId().getHost(), container.getResource());

			for (Collection<ContainerRequest> requestCollection : requestCollections) {
				request = requestCollection.iterator().next();
				// compare capabilities for exact match
				int sizeToleranceMB = 0;
				if (Math.abs(request.getCapability().getMemory() - container.getResource().getMemory()) <= sizeToleranceMB)
					break;
			}

			if (request != null) {
				/* log */ if (HiWayConfiguration.verbose) WorkflowDriver.Logger.writeToStdout(String.format("Removing container request %s:%d:%d", request.getNodes(), request.getCapability().getVirtualCores(), request.getCapability().getMemory()));

				am.getAmRMClient().removeContainerRequest(request);
				am.getNumAllocatedContainers().incrementAndGet();

				// add to ready containers
				containerQueue.add(container);
			} else {
				/* log */ if (HiWayConfiguration.verbose) WorkflowDriver.Logger.writeToStdout(String.format("Releasing container due to no matching request found. ID %d on node %s with capability %d:%d", container.getId().getContainerId(), container.getNodeId().getHost(), container.getResource().getVirtualCores(), container.getResource().getMemory()));
				am.getAmRMClient().releaseAssignedContainer(container.getId());
			}
		}

		launchTasks();
	}

	@Override
	public void onShutdownRequest() {
        WorkflowDriver.Logger.writeToStdout("Shutdown Request.");
		am.setDone();
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
		for(NodeReport nr : updatedNodes){
			/* log */
            WorkflowDriver.Logger.writeToStdout(String.format("Node update, address: %s health: %s, resources: %s, containers: %s", nr.getHttpAddress(), nr.getHealthReport(), nr.getCapability(), nr.getNumContainers()));
		}
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		if (am.getScheduler() == null) return 0f;
		int totalTasks = am.getScheduler().getNumberOfTotalTasks();
		return (totalTasks == 0) ? 0 : (float) am.getNumCompletedContainers().get() / totalTasks;
	}

	@Override
	public void onError(Throwable e) {
        WorkflowDriver.Logger.writeToStdErr(e.getMessage());
		e.printStackTrace(System.out);
		System.exit(-1);
	}

	/** Offers free containers to the scheduler to receive tasks to run in the container.
	 * Called after having received containers in {@link #onContainersAllocated(List)} and completed containers in {@link #onContainersCompleted(List)}.*/
	private void launchTasks() {

		if (!containerQueue.isEmpty() && !am.getScheduler().nothingToSchedule()) {

			/* acc */ long tic = System.currentTimeMillis();

			// ask scheduler for a task to run in this container
//			TaskInstance task = am.getScheduler().scheduleTaskToContainer(allocatedContainer);
			Map<Container, TaskInstance> assignments = am.getScheduler().scheduleTasksToContainers(containerQueue);

			// remove those containers that have been assigned tasks
			containerQueue.removeAll(assignments.keySet());

			/* acc */ long toc = System.currentTimeMillis();

			for(Map.Entry<Container, TaskInstance> assignment : assignments.entrySet()){
				Container allocatedContainer = assignment.getKey();
				TaskInstance task = assignment.getValue();

				/* log */ if (task.getTries() == 1) addTaskRuntimeToTaskReport(allocatedContainer, tic, task, toc);

				// launch and start the container on a separate thread to keep the main thread unblocked and parallelize the overhead
				LaunchContainerRunnable runnableLaunchContainer = am.launchContainer(allocatedContainer, task);

				// remember what went where
				containerIdToInvocation.put(allocatedContainer.getId(), new HiWayInvocation(task));
				am.taskInstanceByContainer.putIfAbsent(allocatedContainer, task);
				containerIdToRunnable.put(allocatedContainer.getId(), runnableLaunchContainer);

			}

		}
	}


	/** Inform the application master about the task's outcome, release surplus containers (e.g., for speculative copies) */
	private void finalizeRequestedContainer(ContainerStatus containerStatus, int exitStatus, ContainerId containerId) {

		HiWayInvocation invocation = containerIdToInvocation.get(containerStatus.getContainerId());
		TaskInstance finishedTask = invocation.task;

		/* success (exit code of last command in {@link TaskInstance#getInvocScript()}?) */
		if (exitStatus == 0) {

			// this task might have been completed previously (e.g., via speculative replication)
			if (finishedTask.isCompleted()) {
				return;
			}

			finishedTask.setCompleted();

			// hand over task report to application master
			am.evaluateReport(finishedTask, containerId);

			/* log */ for (JsonReportEntry entry : finishedTask.getReport()) am.writeEntryToLog(entry);
			/* log */ long runtime = System.currentTimeMillis() - invocation.timestamp;
			/* log */ JSONObject obj = new JSONObject();
			/* log */ try { obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(runtime)); } catch (JSONException e) { e.printStackTrace(System.out); }
			/* log */ am.writeEntryToLog(new JsonReportEntry(System.currentTimeMillis(), finishedTask.getWorkflowId(), finishedTask.getTaskId(), finishedTask.getTaskName(), finishedTask.getLanguageLabel(), finishedTask.getId(), null, JsonReportEntry.KEY_INVOC_TIME, obj));

			Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskCompleted(finishedTask, containerStatus, runtime);
			for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
                /* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Killing speculative copy of task %s on container %s", finishedTask, toBeReleasedContainer));
                am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
                am.getNumKilledContainers().incrementAndGet();
            }

			// signal task success to the application master
			am.taskSuccess(finishedTask, containerId);

			am.getNumCompletedContainers().incrementAndGet();
		}
		// container returned non-zero exit code
		else {

			am.taskFailure(finishedTask, containerId);
			am.getNumFailedContainers().incrementAndGet();

			if (exitStatus == ExitCode.TERMINATED.getExitCode()) {
                /* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Container was terminated. containerID=%s, state=%s, exitStatus=%d, diagnostics=%s", containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(), containerStatus.getDiagnostics()));
			} else {
                /* log */
                WorkflowDriver.Logger.writeToStdout(String.format("Container completed with failure. containerID=%s, state=%s, exitStatus=%d, diagnostics=%s", containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(), containerStatus.getDiagnostics()));
                Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskFailed(finishedTask, containerStatus);
                for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
                    /* log */
                    WorkflowDriver.Logger.writeToStdout(String.format("Killing speculative copy of task %s on container %s", finishedTask, toBeReleasedContainer));
                    am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
                    am.getNumKilledContainers().incrementAndGet();
                }
            }
        }
	}

	private void addTaskRuntimeToTaskReport(Container allocatedContainer, long tic, TaskInstance task, long toc) {
		JSONObject obj = new JSONObject();
		try {
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		} catch (JSONException e) {
			onError(e);
		}
		task.getReport().add( new JsonReportEntry(
				task.getWorkflowId(),
				task.getTaskId(),
				task.getTaskName(),
				task.getLanguageLabel(),
				task.getId(),
				null,
				HiwayDBI.KEY_INVOC_TIME_SCHED,
				obj));
		task.getReport().add( new JsonReportEntry(task.getWorkflowId(),
				task.getTaskId(),
				task.getTaskName(),
				task.getLanguageLabel(),
				task.getId(),
				null,
				HiwayDBI.KEY_INVOC_HOST,
				allocatedContainer.getNodeId().getHost()));
	}

	/**
	 * Write a JSON log entry informing about completed containers.
	 * @param containerStatus the ContainerStatus of as reported back to the {@link #onContainersCompleted(List)} callback.
	 */
	private void logHiwayEventContainerCompleted(ContainerStatus containerStatus, LaunchContainerRunnable runnable) {

		JSONObject entry = new JSONObject();

		//TODO find out why there are nullpointerexceptions
		TaskResourceConsumption taskResourceConsumption;
		if(runnable == null || runnable.getMonitor() == null){
            WorkflowDriver.Logger.writeToStdErr("runnable " + (runnable==null?"null":runnable) + "cadvisormonitor " + (runnable==null?"no runnable":"no monitor on runnable"));
			taskResourceConsumption = new TaskResourceConsumption();
		} else
			taskResourceConsumption = runnable.getMonitor().getTaskResourceConsumption();

		long containerSizeBytes = runnable != null ? runnable.getContainer().getResource().getMemory() * 1024L * 1024L : 0L;
		long wastageByte = containerSizeBytes - taskResourceConsumption.getMemoryByteMax();
		double wastagePercent = 1. * wastageByte / containerSizeBytes;
		try {
			entry.put("type", "container-completed");
			entry.put("container-id", containerStatus.getContainerId());
			entry.put("state", containerStatus.getState());
			entry.put("exit-code", containerStatus.getExitStatus());
			entry.put("diagnostics", containerStatus.getDiagnostics());
			JSONObject resourceConsumption = new JSONObject();
			resourceConsumption.put("memoryByteMax", Long.toString(taskResourceConsumption.getMemoryByteMax()));
			resourceConsumption.put("memoryByteFifty", Long.toString(taskResourceConsumption.getMemoryByteFifty()));
			resourceConsumption.put("memoryByteNinety", Long.toString(taskResourceConsumption.getMemoryByteNinety()));
			resourceConsumption.put("memoryByteNinetyFive", Long.toString(taskResourceConsumption.getMemoryByteNinetyFive()));
			resourceConsumption.put("memoryWastageByte", Long.toString(wastageByte));
			// this is redundant, maybe remove (wastagePercent = memoryWastageByte/(memoryWastageByte+memoryByteMax))
			resourceConsumption.put("memoryWastagePercent", Double.toString(wastagePercent));
			entry.put("resource-consumption", resourceConsumption);
		} catch (JSONException e) {
			// Called when error comes from RM communications as well as from errors in the callback itself from the app. Calling stop() is the recommended action.
			onError(e);
		}

		//TODO find out why nullptr exceptions occur
		if (containerIdToInvocation.get(containerStatus.getContainerId()) == null)
            WorkflowDriver.Logger.writeToStdErr("No invocation for "+containerStatus.getContainerId());
		else{
			TaskInstance finishedTask = containerIdToInvocation.get(containerStatus.getContainerId()).task;
			am.writeEntryToLog(new JsonReportEntry(System.currentTimeMillis(), finishedTask.getWorkflowId(), finishedTask.getTaskId(), finishedTask.getTaskName(), finishedTask.getLanguageLabel(), finishedTask.getId(), null, HiwayDBI.KEY_HIWAY_EVENT, entry));
		}
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
		// at this point, the container has not yet been offered to the scheduler, so there's no invocation mapped to this container yet.
//		TaskInstance task = containerIdToInvocation.get(container.getId()).task;
		am.writeEntryToLog(new JsonReportEntry(System.currentTimeMillis(), am.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
	}
}
