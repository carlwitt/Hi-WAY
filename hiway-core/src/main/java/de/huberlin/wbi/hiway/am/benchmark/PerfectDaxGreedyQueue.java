/******************************************************************************
 In the Hi-WAY project we propose a novel approach of executing scientific
 workflows processing Big Data, as found in NGS applications, on distributed
 computational infrastructures. The Hi-WAY software stack comprises the func-
 tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 for Apache Hadoop 2.x (YARN).

 List of Contributors:

 Marc Bux (HU Berlin)
 Jörgen Brandt (HU Berlin)
 Hannes Schuh (HU Berlin)
 Ulf Leser (HU Berlin)

 Jörgen Brandt is funded by the European Commission through the BiobankCloud
 project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 research training group SOAMED (GRK 1651).

 Copyright 2014 Humboldt-Universität zu Berlin

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package de.huberlin.wbi.hiway.am.benchmark;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.am.dax.DaxTaskInstance;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.WorkflowScheduler;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Takes {@link de.huberlin.wbi.hiway.am.dax.DaxTaskInstance} tasks and requests memory according the exact amount of their container memory limits.
 */
public class PerfectDaxGreedyQueue extends WorkflowScheduler {

	private final List<DaxTaskInstance> queue = new LinkedList<>();

	/** Maps a resource request's size to the time when it was added to the AM's RM client.
	 * Since the workflow has streaks of different size, there should be only one pending resource request per size to which we match the incoming container of the according size to calculate the response time.
	 * Time is measured in milliseconds since epoch. */
	Map<Integer, Long> requestPublishedTime = new HashMap<>();
	Map<Integer, List<Long>> requestAnswerTimes = new HashMap<>();


	public PerfectDaxGreedyQueue(String workflowName) {
		super(workflowName);
	}

	@Override
	protected void addTask(TaskInstance task) {
		numberOfRemainingTasks++;
		if (task.readyToExecute())
			enqueueResourceRequest(task);
	}

	@Override
	public void enqueueResourceRequest(TaskInstance task) {
		DaxTaskInstance daxTask = (DaxTaskInstance) task;

		// request a little more to be safe
		int memoryMB = Math.min(20000, (int) (daxTask.getPeakMemoryBytes()/1000000+200));

		Priority pri = Priority.newInstance(requestPriority);
		Resource capability = Resource.newInstance(memoryMB, 1);

		AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, new String[0], null, pri, getRelaxLocality());
		unissuedContainerRequests.add(containerRequest);

		// remember when the container request was published
		requestPublishedTime.put(containerRequest.getCapability().getMemory(), System.currentTimeMillis());
		WorkflowDriver.Logger.writeToStdout(String.format("PerfDaxGQ requested %s MB for task %s", containerRequest.getCapability().getMemory(), task.getTaskName()));


		queue.add(daxTask);
		/* log */ WorkflowDriver.Logger.writeToStdout("Added task " + task + " to queue");
	}

	@Override
	public TaskInstance scheduleTaskToContainer(Container container) {
		numberOfRemainingTasks--;
		numberOfRunningTasks++;

		// compute and log the service time (from request to allocation) for this container
		logRequestServiceTime(container);

		// biggest containers first such that we match the biggest possible task to each container.
//		queue.sort((t1,t2)-> - Long.compare(t1.getPeakMemoryBytes(),t2.getPeakMemoryBytes()));

		// search the container in the queue with the lowest wastage (but do not change the order to avoid starvation)
		double optWastedBytes = container.getResource().getMemory()*1e6;
		DaxTaskInstance bestfit = null;
		for(DaxTaskInstance task : queue){
			double wastedBytes = container.getResource().getMemory()*1e6 - task.getPeakMemoryBytes();
			if(wastedBytes >= 0 /* task fits */ && wastedBytes < optWastedBytes){
				optWastedBytes = wastedBytes;
				bestfit = task;
			} else {
				WorkflowDriver.Logger.writeToStdout(String.format("PerfDaxGQ skipped task %s (peak_mem_bytes=%s) for %s MB container", task.getTaskName(), task.getPeakMemoryBytes(), container.getResource().getMemory()));
			}
		}

		if(bestfit!=null){
			WorkflowDriver.Logger.writeToStdout(String.format("PerfDaxGQ Assigned task %s (peak_mem_bytes %s) to container %s@%s (memory %s MB)", bestfit, bestfit.getPeakMemoryBytes(), container.getId(), container.getNodeId().getHost(), container.getResource().getMemory()));
			bestfit.incTries();
		}

		return bestfit;
	}

	private void logRequestServiceTime(Container allocatedContainer){
		int containerSizeMB = allocatedContainer.getResource().getMemory();
		if(requestPublishedTime.containsKey(containerSizeMB)){

			long responseTime = System.currentTimeMillis() - requestPublishedTime.get(containerSizeMB);
			requestAnswerTimes.putIfAbsent(containerSizeMB, new ArrayList<>(100));
			requestAnswerTimes.get(containerSizeMB).add(responseTime);

			// remove the request to make sure it's only used once.
			requestPublishedTime.remove(containerSizeMB);

			WorkflowDriver.Logger.writeToStdout(String.format("allocationWaitTime: container_size_MB=%s, wait_time_ms=%s",containerSizeMB, responseTime));

		} else {
			WorkflowDriver.Logger.writeToStdErr(String.format("Streak design assumption didn't hold, launching container of size %s MB for which I don't have a request in requestPublishedTime. State of requestPublishedTime: %s ",
					containerSizeMB,
					requestPublishedTime.entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining("; "))));
		}
	}

	@Override
	public int getNumberOfReadyTasks() {
		return queue.size();
	}

}
