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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class HiWayConfiguration extends YarnConfiguration {

	public static final String HIWAY_SITE_XML = "hiway-site.xml";

	// Application Master
	public static final String HIWAY_AM_DIRECTORY_BASE = "hiway.am.directory.base";
	public static final String HIWAY_AM_DIRECTORY_BASE_DEFAULT = "/";
	public static final String HIWAY_AM_DIRECTORY_CACHE = "hiway.am.directory.cache";
	public static final String HIWAY_AM_DIRECTORY_CACHE_DEFAULT = "hiway";
	static final String HIWAY_AM_APPLICATION_TYPE = "hiway.am.application.type";
	static final String HIWAY_AM_APPLICATION_TYPE_DEFAULT = "Hi-WAY";
	static final String HIWAY_AM_MEMORY = "hiway.am.memory";
	static final int HIWAY_AM_MEMORY_DEFAULT = 1024;
	static final String HIWAY_AM_PRIORITY = "hiway.am.priority";
	static final int HIWAY_AM_PRIORITY_DEFAULT = 0;
	static final String HIWAY_AM_QUEUE = "hiway.am.queue";
	static final String HIWAY_AM_QUEUE_DEFAULT = "default";
	static final String HIWAY_AM_TIMEOUT = "hiway.am.timeout";
	static final int HIWAY_AM_TIMEOUT_DEFAULT = 604800;
	static final String HIWAY_AM_VCORES = "hiway.am.vcores";
	static final int HIWAY_AM_VCORES_DEFAULT = 1;

	// Scheduling
	public static final String HIWAY_SCHEDULER = "hiway.scheduler";
	public static final HIWAY_SCHEDULERS HIWAY_SCHEDULER_DEFAULT = HiWayConfiguration.HIWAY_SCHEDULERS.greedy;
	public static final String HIWAY_SCHEDULER_TASK_RETRIES = "hiway.am.task.retries";
	public static final int HIWAY_SCHEDULER_TASK_RETRIES_DEFAULT = 1;

	// Worker
	public static final String HIWAY_WORKER_MEMORY = "hiway.worker.memory";
	/** Megabytes **/
	public static final int HIWAY_WORKER_MEMORY_DEFAULT = 1024;
	public static final String HIWAY_WORKER_PRIORITY = "hiway.worker.priority";
	public static final int HIWAY_WORKER_PRIORITY_DEFAULT = 0;
	public static final String HIWAY_WORKER_SHELL_ENV = "hiway.worker.shell.env";
	public static final String HIWAY_WORKER_SHELL_ENV_DEFAULT = "";
	public static final String HIWAY_WORKER_VCORES = "hiway.worker.vcores";
	public static final int HIWAY_WORKER_VCORES_DEFAULT = 1;

	// Provenance and Monitoring
	public static final String HIWAY_MONITORING_CADVISOR_PORT = "hiway.monitoring.cadvisor.port";
	public static final String HIWAY_MONITORING_CADVISOR_PORT_DEFAULT = "22223";
	public static final String HIWAY_DB_NOSQL_BUCKET = "hiway.db.nosql.bucket";
	public static final String HIWAY_DB_NOSQL_PASSWORD = "hiway.db.nosql.password";
	public static final String HIWAY_DB_NOSQL_URLS = "hiway.db.nosql.urls";
	public static final String HIWAY_DB_SQL_PASSWORD = "hiway.db.sql.password";
	public static final String HIWAY_DB_SQL_URL = "hiway.db.sql.url";
	public static final String HIWAY_DB_SQL_USER = "hiway.db.sql.user";
	public static final String HIWAY_DB_TYPE = "hiway.db.type";
	public static final HIWAY_DB_TYPE_OPTS HIWAY_DB_TYPE_DEFAULT = HIWAY_DB_TYPE_OPTS.local;

	// Workflow Language
	static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORME_AM_CLASS = "de.huberlin.wbi.hiway.am.cuneiforme.CuneiformEApplicationMaster";
	static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORMJ_AM_CLASS = "de.huberlin.wbi.hiway.am.cuneiformj.CuneiformJApplicationMaster";
	static final String HIWAY_WORKFLOW_LANGUAGE_DAX_AM_CLASS = "de.huberlin.wbi.hiway.am.dax.DaxApplicationMaster";
	static final String HIWAY_WORKFLOW_LANGUAGE_GALAXY_AM_CLASS = "de.huberlin.wbi.hiway.am.galaxy.GalaxyApplicationMaster";
	static final String HIWAY_WORKFLOW_LANGUAGE_LOG_AM_CLASS = "de.huberlin.wbi.hiway.am.log.LogApplicationMaster";
	public static final String HIWAY_GALAXY_PATH = "hiway.galaxy.path";
	public static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORME_WORKER_CLASS = "de.huberlin.wbi.hiway.am.cuneiforme.CuneiformEWorker";
	public static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORME_SERVER_IP = "hiway.cuneiform.server.ip";
	public static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORMJ_WORKER_CLASS = "de.huberlin.wbi.hiway.common.Worker";
	public static final String HIWAY_WORKFLOW_LANGUAGE_DAX_WORKER_CLASS = "de.huberlin.wbi.hiway.common.Worker";
	public static final String HIWAY_WORKFLOW_LANGUAGE_GALAXY_WORKER_CLASS = "de.huberlin.wbi.hiway.common.Worker";
	public static final String HIWAY_WORKFLOW_LANGUAGE_LOG_WORKER_CLASS = "de.huberlin.wbi.hiway.common.Worker";
	static final Map<String, HIWAY_WORKFLOW_LANGUAGE_OPTS> HIWAY_WORKFLOW_LANGUAGE_EXTS;

	public static boolean debug = false;
	public static boolean verbose = false;

	static {
		HIWAY_WORKFLOW_LANGUAGE_EXTS = new HashMap<>();
		HIWAY_WORKFLOW_LANGUAGE_EXTS.put("cf", HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformJ);
		HIWAY_WORKFLOW_LANGUAGE_EXTS.put("xml", HIWAY_WORKFLOW_LANGUAGE_OPTS.dax);
		HIWAY_WORKFLOW_LANGUAGE_EXTS.put("dax", HIWAY_WORKFLOW_LANGUAGE_OPTS.dax);
		HIWAY_WORKFLOW_LANGUAGE_EXTS.put("ga", HIWAY_WORKFLOW_LANGUAGE_OPTS.galaxy);
		HIWAY_WORKFLOW_LANGUAGE_EXTS.put("log", HIWAY_WORKFLOW_LANGUAGE_OPTS.log);

		addDefaultResource(HIWAY_SITE_XML);
	}

	public HiWayConfiguration() {
		super();
	}
	public HiWayConfiguration(Configuration conf) {
		super(conf);
	}

	public enum HIWAY_DB_TYPE_OPTS {
		local, NoSQL, SQL
	}

	public enum HIWAY_SCHEDULERS {
		c3po, dataAware, greedy, heft, roundRobin, memoryAware
	}

	public enum HIWAY_WORKFLOW_LANGUAGE_OPTS {
		cuneiformE, cuneiformJ, dax, galaxy, log
	}

}
