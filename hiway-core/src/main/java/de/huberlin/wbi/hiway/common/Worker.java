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

	public static void main(String[] args) {
		Worker worker = new Worker();
		try {
			worker.init(args);
			worker.run();
		} catch (ParseException | IOException | JSONException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.exit(0);
	}

	protected static void writeEntryToLog(JsonReportEntry entry, long id) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(id + "_" + Invocation.REPORT_FILENAME), true))) {
			writer.write(entry.toString() + "\n");
		}
	}

	private String appId;
	protected HiWayConfiguration conf;
	private String containerId;
	private boolean determineFileSizes = false;
	// private Path dir;
	private FileSystem hdfs;
	private long id;
	private Set<Data> inputFiles;
	private String invocScript = "";
	private String langLabel;
	private Set<Data> outputFiles;
	private long taskId;
	private String taskName;
	private UUID workflowId;

	public Worker() {
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
			// File stdOutFile = new File(Invocation.STDOUT_FILENAME);
			// File stdErrFile = new File(Invocation.STDERR_FILENAME);
			// stdOutFile.createNewFile();
			// stdErrFile.createNewFile();
			// processBuilder.redirectOutput(stdOutFile);
			// processBuilder.redirectError(stdErrFile);
			processBuilder.inheritIO();
			process = processBuilder.start();
			exitValue = process.waitFor();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return exitValue;
	}

	public void init(String[] args) throws ParseException {
		conf = new HiWayConfiguration();
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		Options opts = new Options();
		opts.addOption("appId", true, "Id of this Container's Application Master.");
		opts.addOption("containerId", true, "Id of this Container.");
		opts.addOption("workflowId", true, "");
		opts.addOption("taskId", true, "");
		opts.addOption("taskName", true, "");
		opts.addOption("langLabel", true, "");
		opts.addOption("id", true, "");
		opts.addOption("input", true, "");
		opts.addOption("output", true, "");
		opts.addOption("size", false, "");
		opts.addOption("invocScript", true, "if set, this parameter provides the Worker with the path to the script that is to be stored in invoc-script");

		CommandLine cliParser = new GnuParser().parse(opts, args);
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
		if (cliParser.hasOption("input")) {
			for (String inputList : cliParser.getOptionValues("input")) {
				String[] inputElements = inputList.split(",");
				String otherContainerId = inputElements[2].equals("null") ? null : inputElements[2];
				Data input = new Data(inputElements[0], otherContainerId);
				input.setInput(Boolean.parseBoolean(inputElements[1]));
				inputFiles.add(input);
			}
		}
		if (cliParser.hasOption("size")) {
			determineFileSizes = true;
		}
		if (cliParser.hasOption("output")) {
			for (String output : cliParser.getOptionValues("output")) {
				outputFiles.add(new Data(output, containerId));
			}
		}
		if (cliParser.hasOption("invocScript")) {
			invocScript = cliParser.getOptionValue("invocScript");
		}
	}

	public void run() throws IOException, JSONException {
		long tic = System.currentTimeMillis();
		stageIn();
		long toc = System.currentTimeMillis();
		JSONObject obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, HiwayDBI.KEY_INVOC_TIME_STAGEIN, obj), id);

		tic = System.currentTimeMillis();
		int exitValue = exec();
		toc = System.currentTimeMillis();

		if (invocScript.length() > 0) {
			try (BufferedReader reader = new BufferedReader(new FileReader(invocScript))) {
				String line;
				StringBuilder sb = new StringBuilder();
				while ((line = reader.readLine()) != null) {
					sb.append(line).append("\n");
				}
				writeEntryToLog(new JsonReportEntry(workflowId, taskId, taskName, langLabel, id, JsonReportEntry.KEY_INVOC_SCRIPT, sb.toString()), id);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, JsonReportEntry.KEY_INVOC_TIME, obj), id);

		tic = System.currentTimeMillis();
		new Data(id + "_" + Invocation.STDOUT_FILENAME, containerId).stageOut();
		new Data(id + "_" + Invocation.STDERR_FILENAME, containerId).stageOut();
		if (exitValue != 0) {
			System.exit(exitValue);
		}
		stageOut();
		toc = System.currentTimeMillis();
		obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, null, HiwayDBI.KEY_INVOC_TIME_STAGEOUT, obj), id);

		new Data(id + "_" + Invocation.REPORT_FILENAME, containerId).stageOut();
	}

	public void stageIn() throws IOException, JSONException {
		for (Data input : inputFiles) {
			long tic = System.currentTimeMillis();
			input.stageIn();
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, input.getLocalPath().toString(),
					HiwayDBI.KEY_FILE_TIME_STAGEIN, obj), id);
			if (determineFileSizes) {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, input.getLocalPath().toString(),
						JsonReportEntry.KEY_FILE_SIZE_STAGEIN, Long.toString((new File(input.getLocalPath().toString())).length())), id);
			}

		}
	}

	public void stageOut() throws IOException, JSONException {
		try (BufferedReader logReader = new BufferedReader(new FileReader(new File(id + "_" + Invocation.REPORT_FILENAME)))) {
			String line;
			while ((line = logReader.readLine()) != null) {
				JsonReportEntry entry = new JsonReportEntry(line);
				if (entry.getKey().equals(JsonReportEntry.KEY_FILE_SIZE_STAGEOUT)) {
					outputFiles.add(new Data(entry.getFile(), containerId));
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		for (Data output : outputFiles) {
			long tic = System.currentTimeMillis();
			output.stageOut();
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, output.getLocalPath().toString(),
					HiwayDBI.KEY_FILE_TIME_STAGEOUT, obj), id);
			if (determineFileSizes) {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, output.getLocalPath().toString(),
						JsonReportEntry.KEY_FILE_SIZE_STAGEOUT, Long.toString((new File(output.getLocalPath().toString())).length())), id);
			}
		}
	}

}
