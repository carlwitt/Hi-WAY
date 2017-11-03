/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.huberlin.wbi.hiway.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import de.huberlin.wbi.hiway.monitoring.Estimate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An abstract implementation of a workflow scheduler.
 * 
 * @author Marc Bux
 * 
 */
public abstract class WorkflowScheduler {

	// State
	private final String workflowName;
	/** a queue of nodes on which containers are to be requested */
	protected final Queue<ContainerRequest> unissuedContainerRequests = new LinkedList<>();
	private final Set<Long> taskIds = new HashSet<>();
	private final Map<String, Long> maxTimestampPerHost = new HashMap<>();

	// Configuration
	protected HiWayConfiguration conf;
	/** Specific to the {@link de.huberlin.wbi.hiway.scheduler.ma.MemoryAware} scheduler. Allows manually setting the amount of memory each task type gets. */
	protected Map<String, Integer> customMemoryMap;
	/** Default amount of memory per YARN container. */
	protected int containerMemoryMegaBytes;
	protected int requestPriority;
	boolean relaxLocality = true;
	private int containerCores;
	protected int maxRetries = 0;

	// Accounting
	protected int numberOfRemainingTasks = 0;
	protected int numberOfRunningTasks = 0;
	private int numberOfFinishedTasks = 0;
	private int numberOfPreviousRunTasks = 0;
	/** Takes care of logging and parsing old logs. */
	protected final de.huberlin.wbi.hiway.monitoring.ProvenanceManager provenanceManager = new de.huberlin.wbi.hiway.monitoring.ProvenanceManager(this);

	protected WorkflowScheduler(String workflowName) {
		this.workflowName = workflowName;
	}

	public void init(HiWayConfiguration conf_, FileSystem hdfs_, int containerMemory_, Map<String, Integer> customMemoryMap_, int containerCores_, int requestPriority_) {
		this.conf = conf_;
		this.maxRetries = conf.getInt(HiWayConfiguration.HIWAY_SCHEDULER_TASK_RETRIES, HiWayConfiguration.HIWAY_SCHEDULER_TASK_RETRIES_DEFAULT);
		this.containerMemoryMegaBytes = containerMemory_;
		this.customMemoryMap = customMemoryMap_;
		this.containerCores = containerCores_;
		this.requestPriority = requestPriority_;
		provenanceManager.setHdfs(hdfs_);
	}

	/** If the task is ready, call {@link #enqueueResourceRequest(TaskInstance)}.
	 * This is used on initialization, but also in case of task failure (to readd the task). */
	protected abstract void addTask(TaskInstance task);

	/** Really put the task into the queue(?) as opposed to adding only if it's ready?  */
	public abstract void enqueueResourceRequest(TaskInstance task);

	/** Call {@link #addTask(TaskInstance)} for each task in tasks. */
	public void addTasks(Collection<TaskInstance> tasks) {
		for (TaskInstance task : tasks) {
			addTask(task);
		}
	}

	/** Offer a YARN resource container to the scheduler to get a task back to run in the container. */
	public abstract TaskInstance scheduleTaskToContainer(Container container);

	/** @return the number of tasks that are eligible for execution. */
	public abstract int getNumberOfReadyTasks();

	/**
	 * Create a request for resources.
	 * @param nodes Must be located on one of the given nodes, or pass empty array.
	 */
	protected ContainerRequest setupContainerAskForRM(String[] nodes, int memoryMegaBytes) {

		// set the priority for the request
		Priority pri = Priority.newInstance(requestPriority);
		// pri.setPriority(requestPriority);

		// set up resource type requirements
		Resource capability = Resource.newInstance(memoryMegaBytes, containerCores);
		// capability.setMemory(containerMemoryMegaBytes);
		// capability.setVirtualCores(containerCores);

		return new ContainerRequest(capability, nodes, null, pri, getRelaxLocality());
	}

	public int getNumberOfTotalTasks() {
		int fin = getNumberOfFinishedTasks();
		int run = getNumberOfRunningTasks();
		int rem = numberOfRemainingTasks;

		return fin + run + rem;
	}

	/** Informs the scheduler about a successfully completed task. The logic on whether more tasks are now ready happens elsewhere.
	 * @return a collection of to be released containers, used by {@link de.huberlin.wbi.hiway.scheduler.c3po.C3PO} for killing speculative task replicas.
	 */
	public Collection<ContainerId> taskCompleted(TaskInstance task, ContainerStatus containerStatus, long runtimeInMs) {

		numberOfRunningTasks--;
		numberOfFinishedTasks++;

		/* log */ WorkflowDriver.writeToStdout("Task " + task + " on container " + containerStatus.getContainerId() + " completed successfully after " + runtimeInMs + " ms");

		return new ArrayList<>();
	}

	/** Inform the scheduler about a task failure. Causes task retry, if the number of maximum retries has not been reached.
	 * @return a collection of to be released containers, used by {@link de.huberlin.wbi.hiway.scheduler.c3po.C3PO} for killing speculative task replicas.
	 */
	public Collection<ContainerId> taskFailed(TaskInstance task, ContainerStatus containerStatus) {
		numberOfRunningTasks--;

		/* log */ WorkflowDriver.writeToStdout("Task " + task + " on container " + containerStatus.getContainerId() + " failed");
		if (task.retry(maxRetries)) {
			/* log */ WorkflowDriver.writeToStdout("Retrying task " + task + ".");
			addTask(task);
		} else {
			/* log */ WorkflowDriver.writeToStdout("Task " + task + " has exceeded maximum number of allowed retries. Aborting workflow.");
			System.exit(-1);
		}

		return new ArrayList<>();
	}

	public void newHost(String nodeId) {
		Map<Long, Estimate.RuntimeEstimate> runtimeEstimates = new HashMap<>();
		for (long taskId : getTaskIds()) {
			runtimeEstimates.put(taskId, new Estimate.RuntimeEstimate());
		}
		provenanceManager.runtimeEstimatesPerNode.put(nodeId, runtimeEstimates);
		maxTimestampPerHost.put(nodeId, 0L);
	}

	public void newTask(long taskId) {
		taskIds.add(taskId);
		for (Map<Long, Estimate.RuntimeEstimate> runtimeEstimates : provenanceManager.runtimeEstimatesPerNode.values()) {
			runtimeEstimates.put(taskId, new Estimate.RuntimeEstimate());
		}
	}

	public HiwayDBI getDbInterface() { return provenanceManager.dbInterface; }

	/** Poll a resource request from the schedulers queue of unissued resource requests (which removes that request from the queue). */
	public ContainerRequest getNextNodeRequest() { return unissuedContainerRequests.remove(); }

	public boolean getRelaxLocality() { return relaxLocality; }

	public int getNumberOfFinishedTasks() { return numberOfFinishedTasks - numberOfPreviousRunTasks; }

	public int getNumberOfRunningTasks() { return numberOfRunningTasks; }

	public void setNumberOfFinishedTasks(int numberOfFinishedTasks) { this.numberOfFinishedTasks = numberOfFinishedTasks; }

	public boolean hasNextNodeRequest() { return !unissuedContainerRequests.isEmpty(); }

	public boolean nothingToSchedule() { return getNumberOfReadyTasks() == 0; }

	public Set<Long> getTaskIds() { return new HashSet<>(taskIds); }

	public Map<String, Long> getMaxTimestampPerHost() { return maxTimestampPerHost; }

	public String getWorkflowName() { return workflowName; }

	public HiWayConfiguration getConf() { return conf; }

	public int getNumberOfPreviousRunTasks() { return numberOfPreviousRunTasks; }

	public void setNumberOfPreviousRunTasks(int numberOfPreviousRunTasks) { this.numberOfPreviousRunTasks = numberOfPreviousRunTasks; }

	public void initializeProvenanceManager() { provenanceManager.initializeProvenanceManager(); }

	public void updateRuntimeEstimates(String runId) { provenanceManager.updateRuntimeEstimates(runId); }

	public Set<String> getNodeIds() { return new HashSet<>(provenanceManager.runtimeEstimatesPerNode.keySet()); }

	public void updateRuntimeEstimate(InvocStat stat) { provenanceManager.updateRuntimeEstimate(stat); }

	public void addEntryToDB(JsonReportEntry entry) { provenanceManager.addEntryToDB(entry); }
}
