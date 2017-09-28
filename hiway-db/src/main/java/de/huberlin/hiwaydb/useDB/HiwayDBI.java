/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Hannes Schuh (HU Berlin)
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
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
package de.huberlin.hiwaydb.useDB;

import java.util.Collection;
import java.util.Set;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public interface HiwayDBI {

	String KEY_FILE_TIME_STAGEIN = "file-time-stagein";
	String KEY_FILE_TIME_STAGEOUT = "file-time-stageout";
	String KEY_HIWAY_EVENT = "hiway-event";
	String KEY_INVOC_HOST = "invoc-host";
	String KEY_INVOC_TIME_SCHED = "invoc-time-sched";
	String KEY_INVOC_TIME_STAGEIN = "invoc-time-stagein";
	String KEY_INVOC_TIME_STAGEOUT = "invoc-time-stageout";
	String KEY_WF_NAME = "wf-name";
	String KEY_WF_TIME = "wf-time";
	String KEY_WF_OUTPUT = "wf-output";

	Set<String> getHostNames();

	Set<Long> getTaskIdsForWorkflow(String workflowName);

	String getTaskName(long taskId);

	Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds);

	Collection<InvocStat> getLogEntriesForTaskOnHostSince(long taskId, String hostName, long timestamp);

	void logToDB(JsonReportEntry entry);

}
