/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
 * Hannes Schuh (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * Jörgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universität zu Berlin
 *
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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An abstract implementation of a static workflow scheduler (i.e., a scheduler that build a static schedule of which task to assign to which resource prior to
 * workflow execution).
 * 
 * @author Marc Bux
 * 
 */
public abstract class StaticScheduler extends WorkflowScheduler {

	/** the tasks per compute node that are ready to execute */
	protected final Map<String, Queue<TaskInstance>> readyTasksByNode;

	/** the assignment of task instances to nodes, this is a n:1 relationship, the order of tasks on a node is not specified */
	protected final Map<TaskInstance, String> schedule;

	protected StaticScheduler(String workflowName) {
		super(workflowName);
		schedule = new HashMap<>();
		readyTasksByNode = new HashMap<>();
		relaxLocality = false;
	}

	@Override
	public void addTasks(Collection<TaskInstance> tasks) {
		if (readyTasksByNode.size() == 0) {
			WorkflowDriver.writeToStdout("No provenance data available for static scheduling. Aborting.");
			System.exit(-1);
		}
		super.addTasks(tasks);
	}

	/**
	 * Looks up the node to which the task is scheduled, creates a resource request and adds it to the node's queue
	 * @param task
	 */
	@Override
	public void addTaskToQueue(TaskInstance task) {
		String node = schedule.get(task);
		unissuedContainerRequests.add(setupContainerAskForRM(new String[]{node}, containerMemoryMegaBytes));
		readyTasksByNode.get(node).add(task);
		if (HiWayConfiguration.verbose)
			WorkflowDriver.writeToStdout("Added task " + task + " to queue " + node);
	}

	@Override
	public TaskInstance getTask(Container container) {
		numberOfRemainingTasks--;
		numberOfRunningTasks++;
		String node = container.getNodeId().getHost();

		if (HiWayConfiguration.verbose)
			WorkflowDriver.writeToStdout("Looking for task on container " + container.getId() + " on node " + node + "; Queue:" + readyTasksByNode.get(node).toString());

		TaskInstance task = readyTasksByNode.get(node).remove();

		WorkflowDriver.writeToStdout("Assigned task " + task + " to container " + container.getId() + "@" + node);
		task.incTries();

		return task;
	}

	@Override
	public int getNumberOfReadyTasks() {
		int readyTasks = 0;
		for (Queue<TaskInstance> queue : readyTasksByNode.values()) {
			readyTasks += queue.size();
		}
		return readyTasks;
	}

	@Override
	protected void newHost(String nodeId) {
		super.newHost(nodeId);
		Queue<TaskInstance> queue = new LinkedList<>();
		readyTasksByNode.put(nodeId, queue);
	}

}
