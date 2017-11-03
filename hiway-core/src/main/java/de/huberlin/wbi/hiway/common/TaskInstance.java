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
package de.huberlin.wbi.hiway.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

/**
 *
 */
public class TaskInstance implements Comparable<TaskInstance> {

	private static int runningId = 1;
	/** this task instance's id */
	protected final long id;
	private final long taskId;

	// Workflow graph related
	/** the id of the workflow this task instance belongs to */
	private final UUID workflowId;
	private final Set<TaskInstance> parentTasks;
	private final Set<TaskInstance> childTasks;
	/** input data */
	private final Set<Data> inputData;
	/** output data */
	private final Set<Data> outputData;
	/** the name and (internal) id of the task's executable (e.g. tar) */
	private final String taskName;
	/** The task report contains a summary of stage in and stage out times. */
	private final Set<JsonReportEntry> report;

	// OS and worker related
	/** the programming language of this task (default: bash) */
	private final String languageLabel;
	// TODO it would be nice to externalize the upwardRank and depth to another class since it doesn't apply to all task instances (either wrapper, or inheritance)
	/** the upward rank of tasks in the workflow */
	private double upwardRank = 0d;
	private int depth = 0;
	/** The commands to be executed by the task.
	 * This can be a multiline string, since the contents are written to a bash script which is then executed within the YARN container. */
	private String command;
	private String invocScript = "";

	// Execution logic
	/** whether this task is completed yet */
	private boolean completed;
	/** the number of times this task has been attempted */
	private int tries = 0;

	protected TaskInstance(UUID workflowId, String taskName, long taskId) {
		this(workflowId, taskName, taskId, ForeignLambdaExpr.LANGID_BASH);
	}

	private TaskInstance(UUID workflowId, String taskName, long taskId, String languageLabel) {
		this(runningId++, workflowId, taskName, taskId, languageLabel);
	}

	public TaskInstance(long id, UUID workflowId, String taskName, long taskId, String languageLabel) {
		this.id = id;
		this.workflowId = workflowId;
		this.taskName = taskName;
		this.taskId = taskId;
		this.languageLabel = languageLabel;

		this.completed = false;
		this.inputData = new HashSet<>();
		this.outputData = new HashSet<>();
		this.report = new HashSet<>();
		this.parentTasks = new HashSet<>();
		this.childTasks = new HashSet<>();
	}

	/** Writes the script file containing the task's {@link #command},
	 * registers the script as local resource, and stages it out (?) */
	public Map<String, LocalResource> buildScriptsAndSetResources(Container container) {
		Map<String, LocalResource> localResources = new HashMap<>();
		try {
			String containerId = container.getId().toString();

			// the script contains the task's command
			File script = new File(String.valueOf(getId()));
			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(script))) {
				scriptWriter.write(getCommand());
				/* log */ if(HiWayConfiguration.debug) WorkflowDriver.Logger.writeToStdout(String.format("Writing task %s script contents %s",this.id, getCommand().replace('\n','-')));
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}

			// stage out the script
			Data scriptData = new Data(script.getPath(), containerId);
			try {
				scriptData.stageOut();
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}

			// register script as local resource
			scriptData.addToLocalResourceMap(localResources);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return localResources;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public long getId() {
		return id;
	}

	public void addChildTask(TaskInstance childTask) {
		childTasks.add(childTask);
	}

	public void addInputData(Data data) {
		inputData.add(data);
	}

	public void addOutputData(Data data) {
		outputData.add(data);
	}

	public void addParentTask(TaskInstance parentTask) {
		parentTasks.add(parentTask);
		this.setDepth(parentTask.getDepth() + 1);
	}

	public long countAvailableLocalData(Container container) throws IOException {
		long sum = 0;
		for (Data input : getInputData()) {
			sum += input.countAvailableLocalData(container);
		}
		return sum;
	}

	public Set<Data> getInputData() {
		return inputData;
	}

	public long countAvailableTotalData() throws IOException {
		long sum = 0;
		for (Data input : getInputData()) {
			sum += input.countAvailableTotalData();
		}
		return sum;
	}

	public Set<TaskInstance> getChildTasks() throws WorkflowStructureUnknownException {
		return childTasks;
	}

	public int getDepth() throws WorkflowStructureUnknownException {
		return depth;
	}

	protected void setDepth(int depth) throws WorkflowStructureUnknownException {
		if (this.depth < depth) {
			this.depth = depth;
			for (TaskInstance child : this.getChildTasks()) {
				child.setDepth(depth + 1);
			}
		}
	}

	public boolean readyToExecute() {
		for (TaskInstance parentTask : parentTasks) {
			if (!parentTask.isCompleted())
				return false;
		}
		return true;
	}

	public boolean isCompleted() {
		return completed;
	}

	public long getTaskId() {
		return taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void incTries() {
		tries++;
	}

	public int getTries() {
		return tries;
	}

	/** @return Whether the number of tries is lower or equal to maxRetries */
	public boolean retry(int maxRetries) {
		return tries <= maxRetries;
	}

	/** This is the name given to the container running the task.
	 * It is useful for obtaining resource usage statistics from the cadvisor service.
	 * This can be used, e.g., in {@link de.huberlin.wbi.hiway.am.NMCallbackHandler#onContainerStatusReceived(ContainerId, ContainerStatus)} */
	public String getDockerContainerName() {
		return String.format("%s_%s", getWorkflowId(), getId());
	}

	public UUID getWorkflowId() {
		return workflowId;
	}

	public double getUpwardRank() throws WorkflowStructureUnknownException {
		return upwardRank;
	}

	public void setUpwardRank(double upwardRank) throws WorkflowStructureUnknownException {
		this.upwardRank = upwardRank;
	}

	public String getInvocScript() {
		return invocScript;
	}

	protected void setInvocScript(String invocScript) {
		this.invocScript = invocScript;
	}

	public String getLanguageLabel() {
		return languageLabel;
	}

	public Set<Data> getOutputData() {
		return outputData;
	}

	public void setCompleted() { completed = true; }

	public Set<JsonReportEntry> getReport() {
		return report;
	}

	@Override
	public String toString() {
		return id + " [" + taskName + "]";
	}

	@Override
	public int compareTo(TaskInstance other) {
		return Long.compare(this.getId(), other.getId());
	}
}
