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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
// is not really hadoop, just the first best tool to concat paths
import org.apache.hadoop.fs.Path;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class DaxTaskInstance extends TaskInstance {

	private final Map<Data, Long> fileSizesByte;
	private double runtimeSeconds = 0;
	private long peakMemoryBytes = 0;

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

		if (runtimeSeconds > 0.) {

			StringBuilder command = new StringBuilder();

			// consume memory of the specified amount
			int memoryMegaBytes = (int) Math.ceil(peakMemoryBytes/1024/1024);
			command.append(String.format("docker run --rm --memory=%sb --name %s -i 192.168.127.11:5000/witt/fake-executor:1.0 %s %s",  getDockerContainerName(), (int) runtimeSeconds, memoryMegaBytes));

			// write fake output files of the specified size
			// this may take some time, so the specified task runtime will be regularly exceeded
			for (Data output : getOutputData()) {
				command.append(String.format("; dd if=/dev/zero of=%s bs=%s count=1", output.getLocalPath(), fileSizesByte.get(output) ));
			}

			return command.toString();
		}
		return super.getCommand();
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
}
