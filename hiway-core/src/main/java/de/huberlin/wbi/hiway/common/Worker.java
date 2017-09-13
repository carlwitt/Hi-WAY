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
package de.huberlin.wbi.hiway.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public class Worker {

	public static void loop(Worker worker, String[] args) {
		int exitValue;
		worker.init(args);
		exitValue = worker.run();
		System.exit(exitValue);
	}

	public static void main(String[] args) {
		Worker.loop(new Worker(), args);
	}

	private static void writeEntryToLog(JsonReportEntry entry) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(Invocation.REPORT_FILENAME), true))) {
			writer.write(entry.toString() + "\n");
		}
	}

	private String appId;
	private HiWayConfiguration conf;
	protected String containerId;
	private boolean determineFileSizes = false;
	private FileSystem hdfs;
	protected long id;
	private final Set<Data> inputFiles;
	private String invocScript = "";
	private String langLabel;
	protected final Set<Data> outputFiles;
	private long taskId;
	private String taskName;
	private UUID workflowId;

	protected Worker() {
		inputFiles = new HashSet<>();
		outputFiles = new HashSet<>();
	}

	private int exec() {
		File script = new File("./" + id);
		script.setExecutable(true);
		ProcessBuilder processBuilder = new ProcessBuilder(script.getPath());
		processBuilder.directory(new File("."));
		processBuilder.environment().remove("MALLOC_ARENA_MAX");
		Process process;
		int exitValue = -1;
		try {
			processBuilder.inheritIO();
			process = processBuilder.start();
			exitValue = process.waitFor();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(System.out);
		}

		return exitValue;
	}

	private void init(String[] args) {

		conf = new HiWayConfiguration();
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		Options opts = new Options();
		opts.addOption("appId", true, "Id of this Container's Application Master.");
		opts.addOption("containerId", true, "Id of this Container.");
		opts.addOption("workflowId", true, "");
		opts.addOption("taskId", true, "");
		opts.addOption("taskName", true, "");
		opts.addOption("langLabel", true, "");
		opts.addOption("id", true, "");
		opts.addOption("size", false, "");
		opts.addOption("invocScript", true, "if set, this parameter provides the Worker with the path to the script that is to be stored in invoc-script");

		CommandLine cliParser;
		try {
			cliParser = new GnuParser().parse(opts, args);
			containerId = cliParser.getOptionValue("containerId");
			appId = cliParser.getOptionValue("appId");
			String hdfsBaseDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE, HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE_DEFAULT);
			String hdfsSandboxDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE, HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE_DEFAULT);
			Path hdfsBaseDirectory = new Path(new Path(hdfs.getUri()), hdfsBaseDirectoryName);
			Data.setHdfsBaseDirectory(hdfsBaseDirectory);
			Path hdfsSandboxDirectory = new Path(hdfsBaseDirectory, hdfsSandboxDirectoryName);
			Path hdfsApplicationDirectory = new Path(hdfsSandboxDirectory, appId);
			Data.setHdfsApplicationDirectory(hdfsApplicationDirectory);
			Data.setHdfs(hdfs);

			workflowId = UUID.fromString(cliParser.getOptionValue("workflowId"));
			taskId = Long.parseLong(cliParser.getOptionValue("taskId"));
			taskName = cliParser.getOptionValue("taskName");
			langLabel = cliParser.getOptionValue("langLabel");
			id = Long.parseLong(cliParser.getOptionValue("id"));
			if (cliParser.hasOption("size")) {
				determineFileSizes = true;
			}
			if (cliParser.hasOption("invocScript")) {
				invocScript = cliParser.getOptionValue("invocScript");
			}
		} catch (ParseException e) {
			e.printStackTrace(System.out);
		}

		try (BufferedReader reader = new BufferedReader(new FileReader(id + "_data"))) {
			int n = Integer.parseInt(reader.readLine());
			for (int i = 0; i < n; i++) {
				String[] inputElements = reader.readLine().split(",");
				String otherContainerId = inputElements.length > 1 ? inputElements[1] : null;
				Data input = new Data(inputElements[0], otherContainerId);
				inputFiles.add(input);
			}
			n = Integer.parseInt(reader.readLine());
			for (int i = 0; i < n; i++) {
				outputFiles.add(new Data(reader.readLine(), containerId));
			}
			File stdout = new File(id + "_" + Invocation.STDOUT_FILENAME);
			if (!stdout.exists()) {
				stdout.createNewFile();
			}
			File stderr = new File(id + "_" + Invocation.STDOUT_FILENAME);
			if (!stderr.exists()) {
				stderr.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	private int run() {
		long tic = System.currentTimeMillis();
		stageIn();
		long toc = System.currentTimeMillis();
		JSONObject obj = new JSONObject();
		try {
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		} catch (JSONException e) {
			e.printStackTrace(System.out);
		}
		try {
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, HiwayDBI.KEY_INVOC_TIME_STAGEIN, obj));
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		// tic = System.currentTimeMillis();
		int exitValue = exec();
		// toc = System.currentTimeMillis();

		if (invocScript.length() > 0) {
			try (BufferedReader reader = new BufferedReader(new FileReader(invocScript))) {
				String line;
				StringBuilder sb = new StringBuilder();
				while ((line = reader.readLine()) != null) {
					sb.append(line).append("\n");
				}
				writeEntryToLog(new JsonReportEntry(workflowId, taskId, taskName, langLabel, id, JsonReportEntry.KEY_INVOC_SCRIPT, sb.toString()));
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

		// obj = new JSONObject();
		// obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		// writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, JsonReportEntry.KEY_INVOC_TIME, obj));

		tic = System.currentTimeMillis();
		try {
			new Data(id + "_" + Invocation.STDOUT_FILENAME, containerId).stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		try {
			new Data(id + "_" + Invocation.STDERR_FILENAME, containerId).stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		stageOut();
		toc = System.currentTimeMillis();
		obj = new JSONObject();
		try {
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		} catch (JSONException e) {
			e.printStackTrace(System.out);
		}
		try {
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, HiwayDBI.KEY_INVOC_TIME_STAGEOUT, obj));
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		(new File(Invocation.REPORT_FILENAME)).renameTo(new File(id + "_" + Invocation.REPORT_FILENAME));
		try {
			new Data(id + "_" + Invocation.REPORT_FILENAME, containerId).stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		return exitValue;
	}

	private void stageIn() {
		for (Data input : inputFiles) {
			long tic = System.currentTimeMillis();
			try {
				input.stageIn();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			try {
				obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			} catch (JSONException e) {
				e.printStackTrace(System.out);
			}
			try {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, input.getLocalPath().toString(),
						HiwayDBI.KEY_FILE_TIME_STAGEIN, obj));
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			if (determineFileSizes) {
				try {
					writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, input.getLocalPath().toString(),
							JsonReportEntry.KEY_FILE_SIZE_STAGEIN, Long.toString((new File(input.getLocalPath().toString())).length())));
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}

		}
	}

	public void stageOut() {
		try (BufferedReader logReader = new BufferedReader(new FileReader(new File(Invocation.REPORT_FILENAME)))) {
			String line;
			while ((line = logReader.readLine()) != null) {
				JsonReportEntry entry = new JsonReportEntry(line);
				if (entry.getKey().equals(JsonReportEntry.KEY_FILE_SIZE_STAGEOUT)) {
					outputFiles.add(new Data(entry.getFile(), containerId));
				}
			}
		} catch (IOException | JSONException e) {
			e.printStackTrace(System.out);
		}
		for (Data output : outputFiles) {
			long tic = System.currentTimeMillis();
			try {
				output.stageOut();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			try {
				obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			} catch (JSONException e) {
				e.printStackTrace(System.out);
			}
			try {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, output.getLocalPath().toString(),
						HiwayDBI.KEY_FILE_TIME_STAGEOUT, obj));
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			if (determineFileSizes) {
				try {
					writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, output.getLocalPath().toString(),
							JsonReportEntry.KEY_FILE_SIZE_STAGEOUT, Long.toString((new File(output.getLocalPath().toString())).length())));
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}
		}
	}

}
