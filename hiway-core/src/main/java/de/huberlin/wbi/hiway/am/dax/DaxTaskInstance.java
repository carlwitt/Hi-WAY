/* ******************************************************************************
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
package de.huberlin.wbi.hiway.am.dax;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.util.*;

// is not really hadoop, just the first best tool to concat paths

public class DaxTaskInstance extends TaskInstance {

	private final Map<Data, Long> fileSizesByte;
	/** The amount of time to be spent in the simulated resource usage. */
	private double runtimeSeconds = 0;
	/** The peak amount of memory to be consumed by the simulated resource usage program. */
	private long peakMemoryBytes = 0;

	/** The memory limit on the Docker container that runs the task. This replaces YARN's resource usage monitoring and enforcement,
	 * since resource usage of Docker containers is attributed to the Docker daemon, not the YARN container. */
	private long containerMemoryLimitBytes;

	DaxTaskInstance(UUID workflowId, String taskName) {
		super(workflowId, taskName, Math.abs(taskName.hashCode() + 1));
		fileSizesByte = new HashMap<>();
	}

	void addInputData(Data data, Long fileSize) {
		super.addInputData(data);
		fileSizesByte.put(data, fileSize);
	}

	void addOutputData(Data data, Long fileSize) {
		super.addOutputData(data);
		fileSizesByte.put(data, fileSize);
	}

	@Override
	public String getCommand() {

		assert(runtimeSeconds > 0.);
		assert(containerMemoryLimitBytes > 0);

		// create commands synthetic output files
		StringBuilder createFilesCommand = new StringBuilder();
		for (Data output : getOutputData()) {
			// we assume (for DAX) that all files are just plain file names (no subdirectories) and created within the containers directory
			createFilesCommand.append(String.format("dd if=/dev/zero of=/home/%s bs=%s count=1;", output.getName(), fileSizesByte.get(output) ));
		}

		// consume memory of the specified amount, limit memory usage to the YARN container size
		String command = String.format("docker run  " +
						// limit the containers memory by the YARN container size
						"--memory %s --memory-swap %s " +
						// set a name to allow the application master polling resource information about the container
						"--name %s " +
						// destroy container immediately after finish
						"--rm " +
						"-v $(pwd):/home " +
						"--entrypoint /bin/bash " +
						"192.168.127.11:5000/jess/stress:1.0 " +
						// these commands are executed in the container
						// create synthetic output files
						"-c \"%s " +
						// run for runtimeSeconds
						"/usr/bin/stress --timeout %s " +
						// consume memory, one memory worker, allocating peakMemoryBytes
						"--vm 1 --vm-bytes %s\"",
				containerMemoryLimitBytes,
				containerMemoryLimitBytes,
				getDockerContainerName(),
				createFilesCommand.toString(),
				(int) runtimeSeconds,
				peakMemoryBytes);

		return command;
	}

	/**
	 * Infers the memory limit of the docker container (see {@link #getCommand()} from the amount memory allocated to the YARN container.
	 */
	@Override
	public Map<String, LocalResource> buildScriptsAndSetResources(Container container) {

		// inform the task about its memory limits (needed to build the command)
		setContainerMemoryLimitBytes(container.getResource().getMemory()*1024L*1024L);
		// this calls getCommand() which requires #containerMemoryLimitBytes to be set correctly.
		return super.buildScriptsAndSetResources(container);

	}

	@Override
	public Set<Data> getInputData() {
		if (runtimeSeconds > 0) {
			Set<Data> intermediateData = new HashSet<>();
			for (Data input : super.getInputData()) {
				if (input.getContainerId() != null) {
					intermediateData.add(input);
				}
			}
			return intermediateData;
		}
		return super.getInputData();
	}

	void setRuntimeSeconds(double runtimeSeconds) {
		this.runtimeSeconds = runtimeSeconds;
	}

	void setPeakMemoryConsumption(long peakMemoryBytes){
		this.peakMemoryBytes = peakMemoryBytes;
	}

	public void setContainerMemoryLimitBytes(long bytes) {
		containerMemoryLimitBytes = bytes;
	}

}
