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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.json.JSONObject;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.google.gson.Gson;

import de.huberlin.hiwaydb.LogToDB.InvocDoc;
import de.huberlin.hiwaydb.LogToDB.WfRunDoc;
import de.huberlin.hiwaydb.dal.Accesstime;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public class HiwayDBNoSQL implements HiwayDBI {

	private final List<URI> dbURLs;
	private final String password;
	private final String bucket;

	private final String dbURLSQL;
	private final String passwordSQL;
	private final String usernameSQL;
	private String wfName;
	private String runIDat;
	private final String config;

	private CouchbaseClient client = null;
	private final Gson gson;
	private long dbVolume;

	private SessionFactory dbSessionFactory = null;

	public HiwayDBNoSQL(String bucket, String password, List<URI> dbURLs, String usernameSQL, String passwordSQL, String dbURLSQL) {
		this.bucket = bucket;
		this.password = password;
		this.dbURLs = dbURLs;

		this.usernameSQL = usernameSQL;
		this.passwordSQL = passwordSQL;
		this.dbURLSQL = dbURLSQL;
		this.wfName = "";
		this.runIDat = "";
		this.config = "nix";

		gson = new Gson();

		getConnection();

		View view = client.getView("Workflow", "WfRunCount");

		Query query = new Query();

		// Query the Cluster
		ViewResponse result = client.query(view, query);

		for (ViewRow row : result) {
			// Use Google GSON to parse the JSON into a HashMap
			dbVolume = Long.parseLong(row.getValue(), 10);
		}
	}

	private void getConnection() {
		try {
			System.out.println("connecting to Couchbase NEUE Tocks gesetzt, bucket: " + this.bucket + " pwd:" + this.password);
			client = new CouchbaseClient(this.dbURLs, this.bucket, this.password);

		} catch (Exception e) {
			System.out.println("hiwayDBNoSQL |Error connecting to Couchbase: " + e.getMessage());
		}
	}

	@Override
	public void logToDB(JsonReportEntry entry) {
		Long tick = System.currentTimeMillis();
		String i = lineToDB(entry);

		if (!i.isEmpty()) {
			Long tock = System.currentTimeMillis();
			saveAccessTime(tick, tock, entry.toString().length(), "JsonReportEntryToDB", i);
		} else {
			System.out.println("hiwayDBNoSQL |Fehler...");
		}
	}

	@Override
	public Set<String> getHostNames() {
		Long tick = System.currentTimeMillis();
		Set<String> tempResult = getHostNamesTemp();
		Long tock = System.currentTimeMillis();

		saveAccessTime(tick, tock, tempResult.size(), "getHostNames", null);

		return tempResult;
	}

	private Set<String> getHostNamesTemp() {
		if (client == null) {
			getConnection();
		}
		View view = client.getView("Invoc", "getHostNames");

		// Set up the Query object
		Query query = new Query();

		// We the full documents and only the top 20
		query.setIncludeDocs(false).setGroup(true).setGroupLevel(1);

		// Query the Cluster
		ViewResponse result = client.query(view, query);

		Set<String> tempResult = new HashSet<>();
		// Iterate over the found documents
		for (ViewRow row : result) {
			String x = row.getKey();
			if (x != null && !x.equals("null")) {
				tempResult.add(x);
			}
		}

		return tempResult;
	}

	private String lineToDB(JsonReportEntry logEntryRow) {

		try {

			System.out.println("hiwayDBNoSQL | lineToDB:" + logEntryRow.toString());

			String runID = null;

			if (logEntryRow.getRunId() != null) {
				runID = logEntryRow.getRunId().toString();

				this.runIDat = runID;
			}

			Long invocID = (long) 0;

			if (logEntryRow.hasInvocId()) {
				invocID = logEntryRow.getInvocId();
			}

			Long timestampTemp = logEntryRow.getTimestamp();

			String filename = null;
			if (logEntryRow.getFile() != null) {
				filename = logEntryRow.getFile();
			}

			InvocDoc invocDocument = null;
			WfRunDoc wfRunDocument = null;
			Set<Long> taskIDsforWfRun = new HashSet<>();

			if (runID != null) {
				String wfRunDoc = (String) client.get(runID);

				if (wfRunDoc != null) {
					wfRunDocument = gson.fromJson(wfRunDoc, WfRunDoc.class);

					taskIDsforWfRun = wfRunDocument.getTaskIDs();
				} else {
					wfRunDocument = new WfRunDoc();
					wfRunDocument.setRunId(runID);
				}
			}

			long taskID = 0;
			if (logEntryRow.getTaskId() != null) {
				taskID = logEntryRow.getTaskId();
				taskIDsforWfRun.add(taskID);
			}

			String documentId = runID + "_" + invocID;
			String documentJSON = (String) client.get(documentId);
			String key = logEntryRow.getKey();

			if (invocID != 0) {
				if (documentJSON != null) {
					invocDocument = gson.fromJson(documentJSON, InvocDoc.class);
					invocDocument.setInvocId(invocID);
					invocDocument.setRunId(runID);
					invocDocument.setTimestamp(timestampTemp);
				} else {
					invocDocument = new InvocDoc();
					invocDocument.setInvocId(invocID);
					invocDocument.setRunId(runID);
					invocDocument.setTimestamp(timestampTemp);
				}
			}

			Map<String, HashMap<String, Long>> files = null;
			HashMap<String, Long> oneFile = null;
			JSONObject valuePart;
			Map<String, String> hiwayEvents = null;
			if (wfRunDocument != null) {
				hiwayEvents = wfRunDocument.getHiwayEvent();
			}

			if (invocDocument != null && invocID != 0) {
				invocDocument.setTaskId(taskID);
				invocDocument.setLang(logEntryRow.getLang());
				invocDocument.setTaskname(logEntryRow.getTaskName());
				files = invocDocument.getFiles();

				if (filename != null) {
					oneFile = files.get(filename);
				}
			}

			if (oneFile == null) {
				oneFile = new HashMap<>();
			}

			switch (key) {
			case HiwayDBI.KEY_INVOC_HOST:
				if (invocDocument != null)
					invocDocument.setHostname(logEntryRow.getValueRawString());
				break;
			case "wf-name":
				if (wfRunDocument != null)
					wfRunDocument.setName(logEntryRow.getValueRawString());
				this.wfName = logEntryRow.getValueRawString();
				break;
			case "wf-time":
				String val = logEntryRow.getValueRawString();
				Long test = Long.parseLong(val, 10);
				if (wfRunDocument != null)
					wfRunDocument.setWfTime(test);
				break;
			case HiwayDBI.KEY_INVOC_TIME_SCHED:
				valuePart = logEntryRow.getValueJsonObj();
				if (invocDocument != null)
					invocDocument.setScheduleTime(GetTimeStat(valuePart));
				break;
			case JsonReportEntry.KEY_INVOC_STDERR:
				if (invocDocument != null)
					invocDocument.setStandardError(logEntryRow.getValueRawString());
				break;
			case JsonReportEntry.KEY_INVOC_SCRIPT:
				if (invocDocument != null) {
					Map<String, String> input = invocDocument.getInput();
					input.put("invoc-exec", logEntryRow.getValueRawString());
					invocDocument.setInput(input);
				}
				break;
			case JsonReportEntry.KEY_INVOC_OUTPUT:
				valuePart = logEntryRow.getValueJsonObj();
				if (invocDocument != null) {
					Map<String, String> output = invocDocument.getOutput();
					output.put("invoc-output", valuePart.toString());
					invocDocument.setOutput(output);
				}
				break;
			case JsonReportEntry.KEY_INVOC_STDOUT:
				if (invocDocument != null)
					invocDocument.setStandardOut(logEntryRow.getValueRawString());
				break;
			case "invoc-time-stagein":
				valuePart = logEntryRow.getValueJsonObj();
				if (invocDocument != null)
					invocDocument.setRealTimeIn(GetTimeStat(valuePart));
				break;
			case "invoc-time-stageout":
				valuePart = logEntryRow.getValueJsonObj();
				if (invocDocument != null)
					invocDocument.setRealTimeOut(GetTimeStat(valuePart));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEIN:
				valuePart = logEntryRow.getValueJsonObj();
				oneFile.put("realTimeIn", GetTimeStat(valuePart));
				if (files != null)
					files.put(filename, oneFile);
				if (invocDocument != null)
					invocDocument.setFiles(files);
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
				valuePart = logEntryRow.getValueJsonObj();
				oneFile.put("realTimeOut", GetTimeStat(valuePart));
				if (files != null)
					files.put(filename, oneFile);
				if (invocDocument != null)
					invocDocument.setFiles(files);
				break;
			case JsonReportEntry.KEY_INVOC_TIME:
				valuePart = logEntryRow.getValueJsonObj();
				if (invocDocument != null) {
					try {
						invocDocument.setRealTime(GetTimeStat(valuePart));
					} catch (NumberFormatException e) {
						invocDocument.setRealTime(1L);
					}
				}
				break;
			case "file-size-stagein":
				oneFile.put("size", Long.parseLong(logEntryRow.getValueRawString(), 10));
				if (files != null)
					files.put(filename, oneFile);
				if (invocDocument != null)
					invocDocument.setFiles(files);
				break;
			case "file-size-stageout":
				oneFile.put("size", Long.parseLong(logEntryRow.getValueRawString(), 10));
				if (files != null)
					files.put(filename, oneFile);
				if (invocDocument != null)
					invocDocument.setFiles(files);
				break;
			case HiwayDBI.KEY_HIWAY_EVENT:
				valuePart = logEntryRow.getValueJsonObj();
				if (hiwayEvents != null)
					hiwayEvents.put(valuePart.get("type").toString(), valuePart.toString());
				if (wfRunDocument != null)
					wfRunDocument.setHiwayEvent(hiwayEvents);
				break;
			default:
			}

			if (invocDocument != null) {
				client.set(documentId, gson.toJson(invocDocument));
			}
			
			client.set(runID, gson.toJson(wfRunDocument));
			return key;
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
			return "";
		}
	}

	private static Long GetTimeStat(JSONObject valuePart) {

		return Long.parseLong(valuePart.get("realTime").toString(), 10);
	}

	@Override
	public Set<Long> getTaskIdsForWorkflow(String workflowName) {
		Long tick = System.currentTimeMillis();
		Set<Long> tempResult = getTaskIdsForWorkflowTemp(workflowName);
		Long tock = System.currentTimeMillis();

		if (tempResult.size() > 0) {
			saveAccessTime(tick, tock, 1, "getTaskIdsForWorkflow", null);
		} else {
			saveAccessTime(tick, tock, 0, "getTaskIdsForWorkflow", null);
		}

		return tempResult;
	}

	private Set<Long> getTaskIdsForWorkflowTemp(String workflowName) {
		if (client == null) {
			getConnection();
		}
		View view = client.getView("Workflow", "getTaskIdsForWorkflow");

		// Set up the Query object
		Query query = new Query();
		query.setIncludeDocs(true).setLimit(1).setKey(workflowName);

		// Query the Cluster
		ViewResponse result = client.query(view, query);

		WfRunDoc wfRun = null;
		for (ViewRow row : result) {
			wfRun = gson.fromJson((String) row.getDocument(), WfRunDoc.class);
		}

		if (wfRun != null) {
			return wfRun.getTaskIDs();
		}

		return new HashSet<>();
	}

	@Override
	public String getTaskName(long taskId) {
		Long tick = System.currentTimeMillis();
		String workflowName = getTaskNameTemp(taskId);
		Long tock = System.currentTimeMillis();

		if (!workflowName.equals("")) {
			saveAccessTime(tick, tock, 1, "getTaskname", null);
		} else {
			saveAccessTime(tick, tock, 0, "getTaskname", null);
		}

		return workflowName;
	}

	private String getTaskNameTemp(long taskId) {
		if (client == null) {
			getConnection();
		}

		View view = client.getView("Invoc", "getTaskname");
		// Set up the Query object
		Query query = new Query();
		query.setIncludeDocs(false).setLimit(1).setKey("" + taskId + "");
		// Query the Cluster
		ViewResponse result = client.query(view, query);

		String name = "";
		for (ViewRow row : result) {
			name = row.getValue();
		}

		return name;
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds) {
		Long tick = System.currentTimeMillis();
		if (client == null) {
			getConnection();
		}

		View view = client.getView("Invoc", "getLogEntriesForTasks");
		
		// Set up the Query object
		Query query = new Query();
		String keys = "[";
		for (Long id : taskIds) {
			keys += "[" + id.toString() + "],";
		}
		keys = keys.substring(0, keys.length() - 2);
		keys += "]]";
		query.setIncludeDocs(true).setKeys(keys);
		
		// Query the Cluster
		ViewResponse result = client.query(view, query);
		Long tock = System.currentTimeMillis();
		saveAccessTime(tick, tock, 1, "getLogEntriesForTasks", null);

		return createInvocStat(result);
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTaskOnHostSince(long taskId, String hostName, long timestamp) {
		Long tick = System.currentTimeMillis();

		Collection<InvocStat> stats = getLogEntriesForTaskOnHostSinceTemp(taskId, hostName, timestamp);

		Long tock = System.currentTimeMillis();

		saveAccessTime(tick, tock, stats.size(), "getLogEntriesForTaskOnHostSince", null);

		return stats;
	}

	private Collection<InvocStat> getLogEntriesForTaskOnHostSinceTemp(long taskId, String hostName, long timestamp) {
		if (client == null) {
			getConnection();
		}

		View view = client.getView("Invoc", "getLogEntriesForTaskOnHostSince");

		// Set up the Query object
		Query query = new Query();

		query.setIncludeDocs(true).setRange(ComplexKey.of(taskId, hostName, timestamp), ComplexKey.of(taskId, hostName, 999999999999999999L));

		// Query the Cluster
		ViewResponse result = client.query(view, query);

		return createInvocStat(result);
	}

	private static Set<InvocStat> createInvocStat(ViewResponse result) {
		InvocDoc invocDocument = new InvocDoc();
		Set<InvocStat> tempResult = new HashSet<>();
		Gson gson = new Gson();

		InvocStat temp = null;
		// Iterate over the found documents
		for (ViewRow row : result) {

			// System.out.println("resrow: "+ row.getValue()) ;
			invocDocument = gson.fromJson((String) row.getDocument(), InvocDoc.class);

			temp = new InvocStat(invocDocument.getRunId(), invocDocument.getTaskId());

			if (invocDocument.getHostname() != null && invocDocument.getTaskId() != 0 && invocDocument.getRealTime() != null) {
				temp.setHostName(invocDocument.getHostname());
				temp.setRealTime(invocDocument.getRealTime(), invocDocument.getTimestamp());

				Map<String, HashMap<String, Long>> output = invocDocument.getFiles();

				List<FileStat> fileStatout = new ArrayList<>();
				List<FileStat> fileStatin = new ArrayList<>();
				FileStat file = null;
				Long in = 0L;
				Long out = 0L;

				for (Entry<String, HashMap<String, Long>> val : output.entrySet()) {
					file = new FileStat();
					file.setFileName(val.getKey());
					in = val.getValue().get("realTimeIn");
					out = val.getValue().get("realTimeOut");

					file.setSize(val.getValue().get("size"));

					if (in != null) {
						file.setRealTime(in);
						fileStatin.add(file);
					}

					if (out != null) {
						file.setRealTime(out);
						fileStatout.add(file);
					}

				}
				temp.setOutputfiles(fileStatout);
				temp.setInputfiles(fileStatin);

				tempResult.add(temp);
			}

		}
		return tempResult;
	}

	private SessionFactory getSQLSession() {
		try {
			Configuration configuration = new Configuration();
			String url = this.dbURLSQL.substring(0, this.dbURLSQL.lastIndexOf("/")) + "/messungen";
			System.out.println(url);

			configuration.setProperty("hibernate.connection.url", url);
			configuration.setProperty("hibernate.connection.username", this.usernameSQL);
			configuration.setProperty("hibernate.connection.password", this.passwordSQL);
			configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLInnoDBDialect");
			configuration.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");
			configuration.setProperty("connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");
			configuration.setProperty("hibernate.transaction.factory_class", "org.hibernate.transaction.JDBCTransactionFactory");
			configuration.setProperty("hibernate.current_session_context_class", "thread");
			configuration.setProperty("hibernate.initialPoolSize", "10");
			configuration.setProperty("hibernate.c3p0.min_size", "5");
			configuration.setProperty("hibernate.c3p0.max_size", "300");
			configuration.setProperty("hibernate.maxIdleTime", "3600");
			configuration.setProperty("hibernate.c3p0.maxIdleTimeExcessConnections", "300");
			configuration.setProperty("hibernate.c3p0.timeout", "330");
			configuration.setProperty("hibernate.c3p0.idle_test_period", "300");
			configuration.setProperty("hibernate.c3p0.max_statements", "3000");
			configuration.setProperty("hibernate.c3p0.maxStatementsPerConnection", "20");
			configuration.setProperty("hibernate.c3p0.acquire_increment", "1");
			configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Accesstime.class);

			StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties());
			SessionFactory sessionFactory = configuration.buildSessionFactory(builder.build());
			return sessionFactory;
		} catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

	}

	private void saveAccessTime(long tick, long tock, long returnVolume, String funktion, String key) {
		if (this.wfName.contains("NOAT")) {
			return;
		}
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		Session sess = dbSessionFactory.openSession();
		Transaction tx = null;

		// Non-managed environment idiom with getCurrentSession()
		try {
			tx = sess.beginTransaction();
			Accesstime at = new Accesstime();
			at.setTick(tick);
			at.setFunktion(funktion);
			at.setInput("noSQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);
			if (key != null) {
				at.setKeyinput(key);
			}
			at.setReturnvolume(returnVolume);
			at.setTock(tock);
			at.setTicktockdif(tock - tick);
			at.setRunId(this.runIDat);
			at.setWfName(this.wfName);
			sess.save(at);
			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			sess.close();
		}

	}

}
