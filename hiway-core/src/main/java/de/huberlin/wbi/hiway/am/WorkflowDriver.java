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
package de.huberlin.wbi.hiway.am;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;
import de.huberlin.wbi.hiway.scheduler.WorkflowScheduler;
import de.huberlin.wbi.hiway.scheduler.c3po.C3PO;
import de.huberlin.wbi.hiway.scheduler.gq.GreedyQueue;
import de.huberlin.wbi.hiway.scheduler.heft.HEFT;
import de.huberlin.wbi.hiway.scheduler.ma.MemoryAware;
import de.huberlin.wbi.hiway.scheduler.rr.RoundRobin;

/**
 * Base class for the different Application Masters corresponding to the supported workflow languages like
 * Cuneiform {@link de.huberlin.wbi.hiway.am.cuneiforme.CuneiformEApplicationMaster}
 * and DAX {@link de.huberlin.wbi.hiway.am.dax.DaxApplicationMaster}.
 */
public abstract class WorkflowDriver {



	// Hadoop interface
	/** a handle to interact with the YARN ResourceManager */
	private AMRMClientAsync<ContainerRequest> amRMClient;
	/** a listener for processing the responses from the NodeManagers */
	private NMCallbackHandler containerListener;
	/** a handle to communicate with the YARN NodeManagers */
	private NMClientAsync nmClientAsync;
	/** the yarn tokens to be passed to any launched containers */
	private ByteBuffer allTokens;
	/** a handle to the hdfs */
	private FileSystem hdfs;
	private Path hdfsApplicationDirectory;

	// Scheduling
	/** the workflow scheduler, as defined at workflow launch time */
	private WorkflowScheduler scheduler;
	private HiWayConfiguration.HIWAY_SCHEDULERS schedulerName;
	private final Map<String, Integer> customMemoryMap = new HashMap<>();

	// Resource management
	/** the memory and number of virtual cores to request for the container on which the workflow tasks are launched */
	private int containerMemory = 4096;
	private int maxMem;
	private int maxCores;
	private int containerCores = 1;
	/** priority of the container request */
	private int requestPriority;

	// Application Master self-management
	private final HiWayConfiguration conf;
	/** Remembers the mapping to subsequently retrieve task information when being informed about containers, e.g., in {@link NMCallbackHandler#onContainerStatusReceived(ContainerId, ContainerStatus)}*/
	final ConcurrentMap<Container, TaskInstance> taskInstanceByContainer = new ConcurrentHashMap<>();
	/** a list of threads, one for each container launch, to make container launching non-blocking */
    private final List<Thread> launchThreads = new ArrayList<>();
	/** ??? */
    protected final Map<String, Data> files = new HashMap<>();

	// Execution logic
	private Data workflowFile;
	private Path workflowPath;
	/** A unique id used to identify a run of a workflow. */
	private final UUID runId;
	/** the internal id assigned to this application by the YARN ResourceManager */
	private String appId;
	/** flags denoting workflow execution has finished and been successful */
	private volatile boolean done;
	private volatile boolean success;

	// OS and worker related
	/** environment variables to be passed to any launched containers */
	private final Map<String, String> shellEnv = new HashMap<>();
	/** Don't know what it does, but it is passed to the ContainerLaunchContext in {@link LaunchContainerRunnable} as --size */
	private boolean determineFileSizes = false;

	// Reporting
	private Path summaryPath;
	protected final Logger logger = new Logger(this);

	protected WorkflowDriver() {
		conf = new HiWayConfiguration();
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		runId = UUID.randomUUID();
	}

	/**
	 * The main routine.
	 *
	 * @param appMaster The Application Master
	 * @param args Command line arguments passed to the ApplicationMaster.
	 */
	public static void launch(WorkflowDriver appMaster, String[] args) {
		boolean result = false;
		try {
			/* log */ Logger.writeToStdout("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) System.exit(0);
			result = appMaster.run();
		} catch (Throwable t) {
			/* log */ Logger.writeToStdout("Error running ApplicationMaster");
			t.printStackTrace();
			System.exit(-1);
		}
		if (result) {
			/* log */ Logger.writeToStdout("Application Master completed successfully. Exiting");
			System.exit(0);
		} else {
			/* log */ Logger.writeToStdout("Application Master failed. Exiting");
			System.exit(2);
		}
	}

	public boolean init(String[] args) throws ParseException, IOException, JSONException {

		DefaultMetricsSystem.initialize("ApplicationMaster");

		Options opts = new Options();
		opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("u", "summary", true, "The name of the json summary file. No file is created if this parameter is not specified.");
		opts.addOption("m", "memory", true, "The amount of memory (in MB) to be allocated per worker container. Overrides settings in hiway-site.xml.");
		opts.addOption("c", "custom", true, "The name of an (optional) JSON file, in which custom amounts of memory can be specified per task.");
		opts.addOption("s", "scheduler", true, "The scheduling policy that is to be employed. Valid arguments: " + Arrays.toString(HiWayConfiguration.HIWAY_SCHEDULERS.values()) + ". Overrides settings in hiway-site.xml.");
		opts.addOption("d", "debug", false, "Provide additional logs and information for debugging");
		opts.addOption("v", "verbose", false, "Increase verbosity of output / reporting.");
		opts.addOption("appid", true, "Id of this Application Master.");

		opts.addOption("h", "help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			Logger.printUsage(opts);
			throw new IllegalArgumentException("No args specified for application master to initialize");
		}

		if (cliParser.getArgs().length == 0) {
			Logger.printUsage(opts);
			throw new IllegalArgumentException("No workflow file specified.");
		}

		if (!cliParser.hasOption("appid")) {
			throw new IllegalArgumentException("No id of Application Master specified");
		}

		if (cliParser.hasOption("verbose")) {
			HiWayConfiguration.verbose = true;
		}

		appId = cliParser.getOptionValue("appid");
		try {
			logger.statLog = new BufferedWriter(new FileWriter(appId + ".log"));
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		if (cliParser.hasOption("help")) {
			Logger.printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			Logger.dumpOutDebugInfo();
			HiWayConfiguration.debug = true;
		}

		if (cliParser.hasOption("summary")) {
			summaryPath = new Path(cliParser.getOptionValue("summary"));
		}

		String hdfsBaseDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE, HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE_DEFAULT);
		String hdfsSandboxDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE, HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE_DEFAULT);
		Path hdfsBaseDirectory = new Path(new Path(hdfs.getUri()), hdfsBaseDirectoryName);
		Data.setHdfsBaseDirectory(hdfsBaseDirectory);
		Path hdfsSandboxDirectory = new Path(hdfsBaseDirectory, hdfsSandboxDirectoryName);
		hdfsApplicationDirectory = new Path(hdfsSandboxDirectory, appId);
		Data.setHdfsApplicationDirectory(hdfsApplicationDirectory);
		Data.setHdfs(hdfs);

		if (cliParser.hasOption("custom")) {
			Data customMemPath = new Data(cliParser.getOptionValue("custom"));
			customMemPath.stageIn();
			StringBuilder sb = new StringBuilder();
			try (BufferedReader in = new BufferedReader(new FileReader(customMemPath.getLocalPath().toString()))) {
				String line;
				while ((line = in.readLine()) != null) {
					sb.append(line);
				}
			}
			JSONObject obj = new JSONObject(sb.toString());
			Iterator<?> keys = obj.keys();
			while (keys.hasNext()) {
				String key = (String) keys.next();
				int minMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
				int desiredMem = obj.getInt(key);
				customMemoryMap.put(key, (desiredMem % minMem) == 0 ? desiredMem : (desiredMem / minMem + 1) * minMem);
			}
		}

		Map<String, String> envs = System.getenv();

		/* this application's attempt id (combination of attemptId and fail count) */
		ApplicationAttemptId appAttemptID;
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException("Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}

		Logger.writeToStdout("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId=" + appAttemptID.getAttemptId());

		String shellEnvs[] = conf.getStrings(HiWayConfiguration.HIWAY_WORKER_SHELL_ENV, HiWayConfiguration.HIWAY_WORKER_SHELL_ENV_DEFAULT);
		for (String env : shellEnvs) {
			env = env.trim();
			int index = env.indexOf('=');
			if (index == -1) {
				shellEnv.put(env, "");
				continue;
			}
			String key = env.substring(0, index);
			String val = "";
			if (index < (env.length() - 1)) {
				val = env.substring(index + 1);
			}
			shellEnv.put(key, val);
		}

		String workflowParam = cliParser.getArgs()[0];
		try {
			workflowPath = new Path(new URI(workflowParam).getPath());
		} catch (URISyntaxException e) {
			workflowPath = new Path(workflowParam);
		}

		schedulerName = HiWayConfiguration.HIWAY_SCHEDULERS.valueOf(conf.get(HiWayConfiguration.HIWAY_SCHEDULER,
				HiWayConfiguration.HIWAY_SCHEDULER_DEFAULT.toString()));
		if (cliParser.hasOption("scheduler")) {
			schedulerName = HiWayConfiguration.HIWAY_SCHEDULERS.valueOf(cliParser.getOptionValue("scheduler"));
		}

		containerMemory = conf.getInt(HiWayConfiguration.HIWAY_WORKER_MEMORY, HiWayConfiguration.HIWAY_WORKER_MEMORY_DEFAULT);
		if (cliParser.hasOption("memory")) {
			containerMemory = Integer.parseInt(cliParser.getOptionValue("memory"));
		}

		containerCores = conf.getInt(HiWayConfiguration.HIWAY_WORKER_VCORES, HiWayConfiguration.HIWAY_WORKER_VCORES_DEFAULT);
		requestPriority = conf.getInt(HiWayConfiguration.HIWAY_WORKER_PRIORITY, HiWayConfiguration.HIWAY_WORKER_PRIORITY_DEFAULT);
		return true;
	}

	/**
	 * Main run function for the application master. Does more initialization (sic!). Then calls {@link #executeWorkflow()}.
	 * @return True if there were no errors
	 */
	private boolean run() throws IOException {
		/* log */ Logger.writeToStdout("Starting ApplicationMaster");

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

		try (DataOutputBuffer dob = new DataOutputBuffer()) {

			credentials.writeTokenStorageToStream(dob);
			// remove the AM->RM token so that containers cannot access it.
			Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
			while (iter.hasNext()) {
				Token<?> token = iter.next();
				if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
					iter.remove();
				}
			}
			allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

			// Resource Manager communications setup
			RMCallbackHandler allocListener = new RMCallbackHandler(this);
			amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
			amRMClient.init(conf);
			amRMClient.start();

			// Node Managers communications setup
			containerListener = new NMCallbackHandler(this);
			nmClientAsync = new NMClientAsyncImpl(containerListener);
			nmClientAsync.init(conf);
			nmClientAsync.start();

			// get workflow file
			if (hdfs.exists(workflowPath)) {
				Path localPath = new Path(workflowPath.getName());
				hdfs.copyToLocalFile(false, workflowPath, localPath);
				workflowPath = localPath;
				workflowFile = new Data(workflowPath);
				workflowFile.stageOut();
			} else {
				// TODO this doesn't work; the path is triggered when running the application e.g., as hiway workflows/test.dax
				// but stageIn then fails, because in the HDFS, there is only test.dax and not workflows/test.dax
				workflowFile = new Data(workflowPath);
				workflowFile.stageIn();
			}

			// Register self with ResourceManager. This will start heartbeating to the RM.
			/* the hostname of the container running the Hi-WAY ApplicationMaster */
			String appMasterHostname = NetUtils.getHostname();
			/* the port on which the ApplicationMaster listens for status updates from clients */
			int appMasterRpcPort = -1;
			/* the tracking URL to which the ApplicationMaster publishes info for clients to monitor */
			String appMasterTrackingUrl = "";
			RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

			// initialize scheduler
			switch (schedulerName) {
				case roundRobin:
				case heft:
					int workerMemory = conf.getInt(YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);
					scheduler = schedulerName.equals(HiWayConfiguration.HIWAY_SCHEDULERS.roundRobin) ? new RoundRobin(getWorkflowName()) : new HEFT(getWorkflowName(), workerMemory / containerMemory);
					break;
				case greedy:
					scheduler = new GreedyQueue(getWorkflowName());
					break;
				case memoryAware:
					scheduler = new MemoryAware(getWorkflowName(), amRMClient);
					break;
				default:
					C3PO c3po = new C3PO(getWorkflowName());
					switch (schedulerName) {
						case dataAware:
							c3po.setConservatismWeight(0.01d);
							c3po.setnClones(0);
							c3po.setPlacementAwarenessWeight(12d);
							c3po.setOutlookWeight(0.01d);
							break;
						default:
							c3po.setConservatismWeight(3d);
							c3po.setnClones(2);
							c3po.setPlacementAwarenessWeight(1d);
							c3po.setOutlookWeight(2d);
					}
					scheduler = c3po;
			}
			scheduler.init(conf, hdfs, containerMemory, customMemoryMap, containerCores, requestPriority);
			scheduler.initializeProvenanceManager();

			/* log */ logger.writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_NAME, getWorkflowName()));
			logger.federatedReport = new Data(appId + ".log");

			// parse workflow, obtain ready tasks
			Collection<TaskInstance> readyTasks = parseWorkflow();

			// scheduler updates runtime estimates for all tasks comprising the workflow
			scheduler.updateRuntimeEstimates(getRunId().toString());

			scheduler.addTasks(readyTasks);

			// Dump out information about cluster capability as seen by the resource manager
			maxMem = response.getMaximumResourceCapability().getMemory();
			maxCores = response.getMaximumResourceCapability().getVirtualCores();
			Logger.writeToStdout("Max mem capabililty of resources in this cluster " + maxMem);

			// A resource ask cannot exceed the max.
			if (containerMemory > maxMem) {
				/* log */ Logger.writeToStdout("Container memory specified above max threshold of cluster." + " Using max value." + ", specified=" + containerMemory + ", max=" + maxMem);
				containerMemory = maxMem;
			}
			if (containerCores > maxCores) {
				/* log */ Logger.writeToStdout("Container vcores specified above max threshold of cluster." + " Using max value." + ", specified=" + containerCores + ", max=" + maxCores);
				containerCores = maxCores;
			}

			// ask for resources until the workflow is done.
			executeWorkflow();
			finish();

		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return success;
	}

	/**
	 * Requests resources asynchronously until the workflow is done.
	 * Could be inlined, but it then drowned in initialization code, this way it's much clearer.
	 * {@link #askForResources()} could maybe inlined, but the {@link de.huberlin.wbi.hiway.am.cuneiforme.CuneiformEApplicationMaster} overrides it.
	 */
	private void executeWorkflow() {
		while (!isDone()) {
            askForResources();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/** Issue all of the scheduler's unissued resource requests. */
	protected void askForResources() {

		while (scheduler.hasNextNodeRequest()) {

			// get the next unissued resource request of the scheduler
			ContainerRequest request = scheduler.getNextNodeRequest();
			// send the request to the YARN Resource Manager (asynchronously)
			amRMClient.addContainerRequest(request);

			/* log */ logger.logContainerRequested(request);
			// remember total containers and memory used so far
			/* acc */
			logger.numRequestedContainers.incrementAndGet();
			/* acc */
			logger.totalContainerMemoryMB.addAndGet(request.getCapability().getMemory());

		}

		/* log */ Logger.writeToStdout("Current application state: requested=" + logger.numRequestedContainers + ", totalContainerMemoryMB=" + logger.totalContainerMemoryMB + ",completed=" + logger.getNumCompletedContainers() + ", failed=" + logger.getNumFailedContainers() + ", killed=" + logger.getNumKilledContainers() + ", allocated=" + logger.getNumAllocatedContainers());
		/* log */ if (HiWayConfiguration.verbose) logger.logOutstandingContainerRequests();

	}

	/**
	 * Adds the succeeded task's children to the to-be-scheduled queue (if they're ready). <br/>
	 * Set's the container ID on all of the task's output {@link Data} objects.  <br/>
	 * Checks if the workflow execution is done (scheduler has neither ready nor running tasks).  <br/>
	 * Is called by the {@link RMCallbackHandler#onContainersCompleted(List)} after receiving (and checking for diagnostics) the container completed message from the Resource Manager.
	 */
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		try {
			for (TaskInstance childTask : task.getChildTasks()) {
				if (childTask.readyToExecute())
					scheduler.enqueueResourceRequest(childTask);
			}
		} catch (WorkflowStructureUnknownException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		for (Data data : task.getOutputData()) {
			data.setContainerId(containerId.toString());
		}
		if (scheduler.getNumberOfReadyTasks() == 0 && scheduler.getNumberOfRunningTasks() == 0) {
			setDone();
		}
	}

	@SuppressWarnings("static-method")
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		String line;

		try {
			Logger.writeToStdout("[script]");
			try (BufferedReader reader = new BufferedReader(new StringReader(task.getCommand()))) {
				int i = 0;
				while ((line = reader.readLine()) != null)
					Logger.writeToStdout(String.format("%02d  %s", ++i, line));
			}

			Data stdoutFile = new Data(task.getId() + "_" + Invocation.STDOUT_FILENAME, containerId.toString());
			stdoutFile.stageIn();

			Logger.writeToStdout("[out]");
			try (BufferedReader reader = new BufferedReader(new FileReader(stdoutFile.getLocalPath().toString()))) {
				while ((line = reader.readLine()) != null)
					Logger.writeToStdout(line);
			}

			Data stderrFile = new Data(task.getId() + "_" + Invocation.STDERR_FILENAME, containerId.toString());
			stderrFile.stageIn();

			Logger.writeToStdout("[err]");
			try (BufferedReader reader = new BufferedReader(new FileReader(stderrFile.getLocalPath().toString()))) {
				while ((line = reader.readLine()) != null)
					Logger.writeToStdout(line);
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		Logger.writeToStdout("[end]");
	}

	protected void finish() {
		/* log */
		logger.writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_TIME, Long.toString(System.currentTimeMillis() - amRMClient.getStartTime())));

		// Join all launched threads needed for when we time out and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				Logger.writeToStdout("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace(System.out);
				System.exit(-1);
			}
		}

		// When the application completes, it should stop all running containers
		Logger.writeToStdout("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application signal to the RM
		Logger.writeToStdout("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		success = true;

		// WorkflowDriver.writeToStdout("Failed Containers: " + numFailedContainers.get());
		// WorkflowDriver.writeToStdout("Completed Containers: " + numCompletedContainers.get());

		int numTotalContainers = scheduler.getNumberOfTotalTasks();

		// WorkflowDriver.writeToStdout("Total Scheduled Containers: " + numTotalContainers);

		if (logger.getNumFailedContainers().get() == 0 && logger.getNumCompletedContainers().get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + logger.getNumCompletedContainers().get() + ", allocated="
					+ logger.getNumAllocatedContainers().get() + ", failed=" + logger.getNumFailedContainers().get() + ", killed=" + logger.getNumKilledContainers().get();
			success = false;
		}

		Collection<String> output = getOutput();
		Collection<Data> outputFiles = getOutputFiles();
		if (outputFiles.size() > 0) {
			String outputs = outputFiles.toString();
			logger.writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_OUTPUT, outputs.substring(1, outputs.length() - 1)));
		}

		try {
			logger.statLog.close();
			logger.federatedReport.stageOut();
			if (summaryPath != null) {
				String stdout = hdfsApplicationDirectory + "/AppMaster.stdout";
				String stderr = hdfsApplicationDirectory + "/AppMaster.stderr";
				String statlog = hdfsApplicationDirectory + "/" + appId + ".log";

				try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryPath.toString()))) {
					JSONObject obj = new JSONObject();
					try {
						obj.put("output", output);
						obj.put("stdout", stdout);
						obj.put("stderr", stderr);
						obj.put("statlog", statlog);
					} catch (JSONException e) {
						e.printStackTrace(System.out);
						System.exit(-1);
					}
					writer.write(obj.toString());
				}
				new Data("AppMaster.stdout").stageOut();
				new Data("AppMaster.stderr").stageOut();
				new Data(summaryPath).stageOut();
			}
		} catch (IOException e) {
			Logger.writeToStdout("Error when attempting to stage out federated output log.");
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException | IOException e) {
			Logger.writeToStdout("Failed to unregister application");
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		amRMClient.stop();
	}

	/**
	 * Reads the stdout and stderr from a task and pastes the result into the application log.
	 */
	void evaluateReport(TaskInstance task, ContainerId containerId) {
		logger.evaluateReport(task, containerId);
	}

	ByteBuffer getAllTokens() {
		return allTokens;
	}

	@SuppressWarnings("rawtypes")
	AMRMClientAsync getAmRMClient() {
		return amRMClient;
	}

	String getAppId() {
		return appId;
	}

	public HiWayConfiguration getConf() {
		return conf;
	}

	NMCallbackHandler getContainerListener() {
		return containerListener;
	}

	int getContainerMemory() {
		return containerMemory;
	}

	public Map<String, Data> getFiles() {
		return files;
	}

	List<Thread> getLaunchThreads() {
		return launchThreads;
	}

	NMClientAsync getNmClientAsync() {
		return nmClientAsync;
	}

	AtomicInteger getNumAllocatedContainers() {
		return logger.getNumAllocatedContainers();
	}

	AtomicInteger getNumCompletedContainers() {
		return logger.getNumCompletedContainers();
	}

	AtomicInteger getNumFailedContainers() {
		return logger.getNumFailedContainers();
	}

	AtomicInteger getNumKilledContainers() {
		return logger.getNumKilledContainers();
	}

	public WorkflowScheduler getScheduler() {
		return scheduler;
	}

	Map<String, String> getShellEnv() {
		return shellEnv;
	}

	protected Data getWorkflowFile() {
		return workflowFile;
	}

	boolean isDetermineFileSizes() {
		return determineFileSizes;
	}

	private String getWorkflowName() {
		return workflowFile.getName();
	}

	public void writeEntryToLog(JsonReportEntry entry) {
		logger.writeEntryToLog(entry);
	}

	public UUID getRunId() {
		return runId;
	}

	protected abstract Collection<TaskInstance> parseWorkflow();

	protected boolean isDone() {
		return done;
	}

	protected Collection<String> getOutput() {
		Collection<String> output = new ArrayList<>();
		for (Data outputFile : getOutputFiles()) {
			output.add(outputFile.getHdfsPath().toString());
		}
		return output;
	}

	protected Collection<Data> getOutputFiles() {
		Collection<Data> outputFiles = new ArrayList<>();

		for (Data data : files.values()) {
			if (data.isOutput()) {
				outputFiles.add(data);
			}
		}

		return outputFiles;
	}

	protected void setDetermineFileSizes() {
		determineFileSizes = true;
	}

	public void setDone() {
		this.done = true;
	}

	/**
	 * Fields and methods concerned with logging and reporting.
	 */
	public static class Logger {
		private final WorkflowDriver workflowDriver;
		/**
		 * the report, in which provenance information is stored
		 */
		Data federatedReport;
		BufferedWriter statLog;
		/**
		 * Format for logging.
		 */
		static final SimpleDateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
		/**
		 * a counter for allocated containers
		 */
		final AtomicInteger numAllocatedContainers = new AtomicInteger();
		/**
		 * a counter for completed containers (complete denotes successful or failed
		 */
		final AtomicInteger numCompletedContainers = new AtomicInteger();
		/**
		 * a counter for failed containers
		 */
		final AtomicInteger numFailedContainers = new AtomicInteger();
		/**
		 * a counter for killed containers
		 */
		final AtomicInteger numKilledContainers = new AtomicInteger();
		/**
		 * a counter for requested containers
		 */
		final AtomicInteger numRequestedContainers = new AtomicInteger();
		/**
		 * a counter for the total allocated memory
		 */
		final AtomicLong totalContainerMemoryMB = new AtomicLong(0);

		public Logger(WorkflowDriver workflowDriver) {
			this.workflowDriver = workflowDriver;
		}

		public static void writeToStdErr(String s) {
			System.err.println(dateFormat.format(new Date()) + " " + s);
		}

		/**
		 * (Failed) attempt to create valid JSON (in contrast to {@link JsonReportEntry#toString()}, which does not quote some attribute name strings).
		 * The problem is that there's no way to get to the actual value of the object. Access is private and both getters are broken.
		 * java.lang.RuntimeException: Value is not a JSON object, but a string. at de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry.getValueJsonObj(JsonReportEntry.java:229)
		 * java.lang.RuntimeException: Value is not a string, but a JSON object. at de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry.getValueRawString(JsonReportEntry.java:239)
		 *
		 * @return a single JSON object, as serialized to a string
		 * @deprecated
		 */
		public static String jsonReportEntryToString(JsonReportEntry entry) {
			StringBuffer buf;

			buf = new StringBuffer();

			buf.append('{');
			buf.append("\"").append(JsonReportEntry.ATT_TIMESTAMP).append("\"").append(':').append(entry.getTimestamp()).append(',');
			buf.append("\"").append(JsonReportEntry.ATT_RUNID).append("\"").append(":\"").append(entry.getRunId()).append("\",");

			if (entry.hasTaskId())
				buf.append("\"").append(JsonReportEntry.ATT_TASKID).append("\"").append(':').append(entry.getTaskId()).append(',');

			if (entry.hasTaskname())
				buf.append("\"").append(JsonReportEntry.ATT_TASKNAME).append("\"").append(':').append("\"").append(entry.getTaskName()).append("\",");

			if (entry.hasLang())
				buf.append("\"").append(JsonReportEntry.ATT_LANG).append("\"").append(':').append("\"").append(entry.getLang()).append("\",");

			if (entry.hasInvocId())
				buf.append("\"").append(JsonReportEntry.ATT_INVOCID).append("\"").append(':').append(entry.getInvocId()).append(',');

			if (entry.hasFile())
				buf.append("\"").append(JsonReportEntry.ATT_FILE).append("\"").append(":\"").append(entry.getFile()).append("\",");

			buf.append("\"").append(JsonReportEntry.ATT_KEY).append("\"").append(":\"").append(entry.getKey()).append("\",");
			// there's no way to get to the .value field (private) both getValueJsonObj and getValueRawString are broken.
//		try {
//			buf.append("\"").append( JsonReportEntry.ATT_VALUE ).append("\"").append( ':' ).append( entry.getValueJsonObj().toString() );
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}
			buf.append('}');

			return buf.toString();
		}

		/**
		 * Reads the stdout and stderr from a task and pastes the result into the application log.
		 */
		void evaluateReport(TaskInstance task, ContainerId containerId) {
			try {
				Data reportFile = new Data(task.getId() + "_" + Invocation.REPORT_FILENAME, containerId.toString());
				reportFile.stageIn();
				Data stdoutFile = new Data(task.getId() + "_" + Invocation.STDOUT_FILENAME, containerId.toString());
				stdoutFile.stageIn();
				Data stderrFile = new Data(task.getId() + "_" + Invocation.STDERR_FILENAME, containerId.toString());
				stderrFile.stageIn();

				// (a) evaluate report
				Set<JsonReportEntry> report = task.getReport();
				try (BufferedReader reader = new BufferedReader(new FileReader(task.getId() + "_" + Invocation.REPORT_FILENAME))) {
					String line;
					while ((line = reader.readLine()) != null) {
						line = line.trim();
						if (line.isEmpty())
							continue;
						report.add(new JsonReportEntry(line));
					}
				}
				try (BufferedReader reader = new BufferedReader(new FileReader(task.getId() + "_" + Invocation.STDOUT_FILENAME))) {
					String line;
					StringBuilder sb = new StringBuilder();
					while ((line = reader.readLine()) != null) {
						sb.append(line.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\"")).append('\n');
					}
					String s = sb.toString();
					if (s.length() > 0) {
						JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
								null, JsonReportEntry.KEY_INVOC_STDOUT, sb.toString());
						report.add(re);
					}
				}
				try (BufferedReader reader = new BufferedReader(new FileReader(task.getId() + "_" + Invocation.STDERR_FILENAME))) {
					String line;
					StringBuilder sb = new StringBuilder();
					while ((line = reader.readLine()) != null) {
						sb.append(line.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\"")).append('\n');
					}
					String s = sb.toString();
					if (s.length() > 0) {
						JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
								null, JsonReportEntry.KEY_INVOC_STDERR, sb.toString());
						report.add(re);
					}
				}

			} catch (Exception e) {
				writeToStdout("Error when attempting to evaluate report of invocation " + task.toString() + ". exiting");
				e.printStackTrace(System.out);
				System.exit(-1);
			}
		}

		public static void writeToStdout(String s) {
			System.out.println(dateFormat.format(new Date()) + " " + s);
		}

		AtomicInteger getNumAllocatedContainers() {
			return numAllocatedContainers;
		}

		AtomicInteger getNumCompletedContainers() {
			return numCompletedContainers;
		}

		AtomicInteger getNumFailedContainers() {
			return numFailedContainers;
		}

		AtomicInteger getNumKilledContainers() {
			return numKilledContainers;
		}

		/**
		 * Helper function to print usage.
		 *
		 * @param opts Parsed command line options.
		 */
		static void printUsage(Options opts) {
			new HelpFormatter().printHelp("hiway [options] workflow", opts);
		}

		/**
		 * If the debug flag is set, dump out contents of current working directory and the environment to stdout for debugging.
		 */
		static void dumpOutDebugInfo() {
			writeToStdout("Dump debug output");
			Map<String, String> envs = System.getenv();
			for (Map.Entry<String, String> env : envs.entrySet()) {
				writeToStdout("System env: key=" + env.getKey() + ", val=" + env.getValue());
			}

			String cmd = "ls -al";
			Runtime run = Runtime.getRuntime();
			Process pr;
			try {
				pr = run.exec(cmd);
				pr.waitFor();

				try (BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()))) {
					String line;
					while ((line = buf.readLine()) != null) {
						writeToStdout("System CWD content: " + line);
					}
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}
		}

		public void writeEntryToLog(JsonReportEntry entry) {
			try {
				// TODO this does not create valid JSON, because the (cuneiform) implementation produces unquoted attribute names in the JSON object.
				// the workaround is to postprocess the log or use a tolerant parser (such as from python package dirtyjson)
				statLog.write(entry.toString());
				statLog.newLine();
				statLog.flush();
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}
			workflowDriver.getScheduler().addEntryToDB(entry);
		}

		void logContainerRequested(ContainerRequest request) {
			JSONObject value = new JSONObject();
			try {
				value.put("type", "container-requested");
				value.put("memory", request.getCapability().getMemory());
				value.put("vcores", request.getCapability().getVirtualCores());
				value.put("nodes", request.getNodes());
				value.put("priority", request.getPriority());
			} catch (JSONException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}

			if (HiWayConfiguration.verbose)
				writeToStdout("Requested container " + request.getNodes() + ":" + request.getCapability().getVirtualCores() + ":"
						+ request.getCapability().getMemory());
			writeEntryToLog(new JsonReportEntry(workflowDriver.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
		}

		void logOutstandingContainerRequests() {
			// information on outstanding container request
			StringBuilder sb = new StringBuilder("Open Container Requests: ");
			Set<String> names = new HashSet<>();
			names.add(ResourceRequest.ANY);
			if (!workflowDriver.getScheduler().getRelaxLocality())
				names = workflowDriver.getScheduler().getDbInterface().getHostNames();
			for (String node : names) {
				List<? extends Collection<ContainerRequest>> requestCollections =
						workflowDriver.getAmRMClient().getMatchingRequests(Priority.newInstance(workflowDriver.requestPriority), node, Resource.newInstance(workflowDriver.maxMem, workflowDriver.maxCores));
				for (Collection<ContainerRequest> requestCollection : requestCollections) {
					ContainerRequest first = requestCollection.iterator().next();
					sb.append(node);
					sb.append(":");
					sb.append(first.getCapability().getVirtualCores());
					sb.append(":");
					sb.append(first.getCapability().getMemory());
					sb.append(":");
					sb.append(requestCollection.size());
					sb.append(" ");
				}
			}
			writeToStdout(sb.toString());
		}
	}
}
