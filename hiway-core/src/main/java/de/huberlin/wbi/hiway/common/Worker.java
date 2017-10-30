/*
 *******************************************************************************
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

	protected String containerId;
	private boolean determineFileSizes = false;
	private FileSystem hdfs;
	protected long id;
	private final Set<Data> inputFiles = new HashSet<>();
	private String invocScript = "";
	private String langLabel;
	protected final Set<Data> outputFiles = new HashSet<>();
	private long taskId;
	private String taskName;
	private UUID workflowId;

	protected Worker() { }

	/**
	 * Initializes the worker, stages input files in, executes the process carrying out this task's work,
	 * and stages the output files and reports out.
	 * @param worker An instance of this class, e.g., {@link de.huberlin.wbi.hiway.am.cuneiforme.CuneiformEWorker}
	 * @param args Command line parameters (to the worker Java process? Set by the application master?)
	 */
	public static void run(Worker worker, String[] args) {

		// parse command line arguments, get input and output file names
		worker.init(args);

		// stage in: get input files from distributed file system
		long tic = System.currentTimeMillis();
		worker.stageIn();
		long toc = System.currentTimeMillis();
		/* log */
		worker.logDuration(HiwayDBI.KEY_FILE_TIME_STAGEIN, toc-tic, null);

		// execute the worker's task
		int exitValue = -1;
		File script = new File("./" + worker.id);
		script.setExecutable(true);
		ProcessBuilder processBuilder = new ProcessBuilder(script.getPath());
		processBuilder.directory(new File("."));
		processBuilder.environment().remove("MALLOC_ARENA_MAX");
		Process process;
//		tic = System.currentTimeMillis();
		try {
			processBuilder.inheritIO();
			process = processBuilder.start();
			exitValue = process.waitFor();
		} catch (IOException | InterruptedException e1) {
			e1.printStackTrace(System.out);
		}
//		toc = System.currentTimeMillis();
		// redundant/conflicting with the application master, which logs invoc-time as the difference between container completed and container started events? {@link RMCallbackHandler#finalizeRequestedContainer}
		/* log logDuration(JsonReportEntry.KEY_INVOC_TIME, toc-tic);*/

		// read and log the invocation script
		/* log */ worker.logInvocationScript();

		// stage out output files and stdout and stderr files, measure time
		Data stdOutData = new Data(worker.id + "_" + Invocation.STDOUT_FILENAME, worker.containerId);
		Data stdErrData = new Data(worker.id + "_" + Invocation.STDERR_FILENAME, worker.containerId);
		tic = System.currentTimeMillis();
		try {
			worker.stageOut();
			stdOutData.stageOut();
			stdErrData.stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		toc = System.currentTimeMillis();
		/* log */
		worker.logDuration(HiwayDBI.KEY_INVOC_TIME_STAGEOUT, toc-tic, null);

		// stage out the report file
		(new File(Invocation.REPORT_FILENAME)).renameTo(new File(worker.id + "_" + Invocation.REPORT_FILENAME));
		try {
			new Data(worker.id + "_" + Invocation.REPORT_FILENAME, worker.containerId).stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		System.exit(exitValue);
	}

	/**
	 * Read command line parameters, input and output file names, and create files for stdout and stderr.
	 * @param args Command line parameters.
	 */
	private void init(String[] args) {

		HiWayConfiguration conf = new HiWayConfiguration();
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		// extract application id, container id, workflow id, task name, invoc script, etc. from command line parameters
		parseCommandLineParameters(args, conf);

		// read input and output file names
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
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		// create files for standard output and standard error streams
		try {
			File stdout = new File(id + "_" + Invocation.STDOUT_FILENAME);
			if (!stdout.exists()) stdout.createNewFile();
			File stderr = new File(id + "_" + Invocation.STDOUT_FILENAME);
			if (!stderr.exists()) stderr.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** extract application id, container id, workflow id, task name, invoc script, etc. from command line parameters */
	private void parseCommandLineParameters(String[] args, HiWayConfiguration conf) {
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
			String appId = cliParser.getOptionValue("appId");
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
			/* log */ logDuration(HiwayDBI.KEY_FILE_TIME_STAGEIN, toc-tic, input.getLocalPath().toString());
			if (determineFileSizes) {
				writeEntryToLogSilent(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, input.getLocalPath().toString(), JsonReportEntry.KEY_FILE_SIZE_STAGEIN, Long.toString((new File(input.getLocalPath().toString())).length())));
			}

		}
	}

	public void stageOut() {
		// determine output files from task report
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
		// stage output files out
		for (Data output : outputFiles) {
			long tic = System.currentTimeMillis();
			try {
				output.stageOut();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			long toc = System.currentTimeMillis();
			/* log */ logDuration(HiwayDBI.KEY_FILE_TIME_STAGEOUT, toc-tic, output.getLocalPath().toString());
			/* log */ if (determineFileSizes) writeEntryToLogSilent(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, id, output.getLocalPath().toString(), JsonReportEntry.KEY_FILE_SIZE_STAGEOUT, Long.toString((new File(output.getLocalPath().toString())).length())));
		}
	}

	private void logDuration(String label, long milliSeconds, String file) {
		JSONObject obj = new JSONObject();
		try {
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(milliSeconds));
		} catch (JSONException e) {
			e.printStackTrace(System.out);
		}
		writeEntryToLogSilent(new JsonReportEntry(System.currentTimeMillis(), workflowId, taskId, taskName, langLabel, id, file, label, obj));
	}

	private void logInvocationScript() {
		if (invocScript.length() > 0) {
			try (BufferedReader reader = new BufferedReader(new FileReader(invocScript))) {
				String line;
				StringBuilder sb = new StringBuilder();
				while ((line = reader.readLine()) != null) {
					sb.append(line).append("\n");
				}
				writeEntryToLogSilent(new JsonReportEntry(workflowId, taskId, taskName, langLabel, id, JsonReportEntry.KEY_INVOC_SCRIPT, sb.toString()));
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	/** Write a message to the task's log. */
	private static void writeEntryToLog(JsonReportEntry entry) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(Invocation.REPORT_FILENAME), true))) {
			writer.write(entry.toString() + "\n");
		}
	}

	/** Write a message to the task's log. Catch and print errors to System.out. */
	private static void writeEntryToLogSilent(JsonReportEntry entry) {
		try {
			writeEntryToLog(entry);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static void main(String[] args) {
		Worker.run(new Worker(), args);
	}
}
