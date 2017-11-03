package de.huberlin.wbi.hiway.monitoring;

import de.huberlin.hiwaydb.useDB.HiwayDB;
import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.hiwaydb.useDB.HiwayDBNoSQL;
import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.scheduler.WorkflowScheduler;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 *
 * Was previously part of the {@link WorkflowScheduler} class.
 */
public class ProvenanceManager {

    private final WorkflowScheduler workflowScheduler;
    public final Map<String, Map<Long, Estimate.RuntimeEstimate>> runtimeEstimatesPerNode = new HashMap<>();
    public HiwayDBI dbInterface;
    private FileSystem hdfs;

    public ProvenanceManager(WorkflowScheduler workflowScheduler) {
        this.workflowScheduler = workflowScheduler;
    }

    public void initializeProvenanceManager() {

        HiWayConfiguration.HIWAY_DB_TYPE_OPTS dbType = HiWayConfiguration.HIWAY_DB_TYPE_OPTS.valueOf(workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_TYPE,
                HiWayConfiguration.HIWAY_DB_TYPE_DEFAULT.toString()));
        switch (dbType) {
            case SQL:
                String sqlUser = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_USER);
                if (sqlUser == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_USER + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                String sqlPassword = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
                if (sqlPassword == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                String sqlURL = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_URL);
                if (sqlURL == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_URL + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                dbInterface = new HiwayDB(sqlUser, sqlPassword, sqlURL);
                break;
            case NoSQL:
                sqlUser = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_USER);
                if (sqlUser == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_USER + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                sqlPassword = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
                if (sqlPassword == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                sqlURL = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_SQL_URL);
                if (sqlURL == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_URL + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                String noSqlBucket = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_NOSQL_BUCKET);
                if (noSqlBucket == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_BUCKET + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                String noSqlPassword = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_NOSQL_PASSWORD);
                if (noSqlPassword == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                String noSqlURIs = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_DB_NOSQL_URLS);
                if (noSqlURIs == null) {
                    /* log */ WorkflowDriver.Logger.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_URLS + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
                    throw new RuntimeException();
                }
                List<URI> noSqlURIList = new ArrayList<>();
                for (String uri : noSqlURIs.split(",")) {
                    noSqlURIList.add(URI.create(uri));
                }
                dbInterface = new HiwayDBNoSQL(noSqlBucket, noSqlPassword, noSqlURIList, sqlUser, sqlPassword, sqlURL);

                break;
            default:
                dbInterface = new de.huberlin.wbi.hiway.common.ProvenanceManager();
                parseLogs();
        }
    }

    /**
     * scan prior workflow execution for performance information that can be used to schedule the current workflow
     * TODO this is always executed on workflow startup, even when not scheduling statically or predictively, should be removed, introduces a little runtime bias.
     */
    private void parseLogs() {
        String hdfsBaseDirectoryName = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE, HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE_DEFAULT);
        String hdfsSandboxDirectoryName = workflowScheduler.getConf().get(HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE, HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE_DEFAULT);
        Path hdfsBaseDirectory = new Path(new Path(getHdfs().getUri()), hdfsBaseDirectoryName);
        Path hdfsSandboxDirectory = new Path(hdfsBaseDirectory, hdfsSandboxDirectoryName);
        try {
            for (FileStatus appDirStatus : getHdfs().listStatus(hdfsSandboxDirectory)) {
                if (appDirStatus.isDirectory()) {
                    Path appDir = appDirStatus.getPath();
                    for (FileStatus srcStatus : getHdfs().listStatus(appDir)) {
                        Path src = srcStatus.getPath();
                        String srcName = src.getName();
                        if (srcName.endsWith(".log")) {
                            Path dest = new Path(appDir.getName());
                            WorkflowDriver.Logger.writeToStdout("Parsing log " + dest.toString());
                            getHdfs().copyToLocalFile(false, src, dest);

                            try (BufferedReader reader = new BufferedReader(new FileReader(new File(dest.toString())))) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    if (line.length() == 0)
                                        continue;
                                    JsonReportEntry entry = new JsonReportEntry(line);
                                    addEntryToDB(entry);
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException | JSONException e) {
            e.printStackTrace(System.out);
            System.exit(-1);
        }
    }

    public void addEntryToDB(JsonReportEntry entry) {
        // WorkflowDriver.writeToStdout("HiwayDB: Adding entry " + entry + " to database.");
        // WorkflowDriver.writeToStdout("HiwayDB: Added entry to database.");
        dbInterface.logToDB(entry);
        // WorkflowDriver.writeToStdout("HiwayDB: Added entry to database.");
    }

    public void updateRuntimeEstimates(String runId) {
        // WorkflowDriver.writeToStdout("HiwayDB: Querying Host Names from database.");
        // WorkflowDriver.writeToStdout("HiwayDB: Retrieved Host Names " + newHostIds.toString() + " from database.");
        // WorkflowDriver.writeToStdout("HiwayDB: Querying Task Ids for workflow " + workflowName + " from database.");
        // WorkflowDriver.writeToStdout("HiwayDB: Retrieved Task Ids " + newTaskIds.toString() + " from database.");


        if (HiWayConfiguration.verbose)
            WorkflowDriver.Logger.writeToStdout("Updating Runtime Estimates.");

        // WorkflowDriver.writeToStdout("HiwayDB: Querying Host Names from database.");
        Collection<String> newHostIds = dbInterface.getHostNames();
        // WorkflowDriver.writeToStdout("HiwayDB: Retrieved Host Names " + newHostIds.toString() + " from database.");
//        newHostIds.removeAll(workflowScheduler.getNodeIds());
        for (String newHostId : newHostIds) {
            workflowScheduler.newHost(newHostId);
        }
        // WorkflowDriver.writeToStdout("HiwayDB: Querying Task Ids for workflow " + workflowName + " from database.");
        Collection<Long> newTaskIds = dbInterface.getTaskIdsForWorkflow(workflowScheduler.getWorkflowName());
        // WorkflowDriver.writeToStdout("HiwayDB: Retrieved Task Ids " + newTaskIds.toString() + " from database.");

        newTaskIds.removeAll(workflowScheduler.getTaskIds());
        for (long newTaskId : newTaskIds) {
            workflowScheduler.newTask(newTaskId);
        }

        for (String hostName : workflowScheduler.getNodeIds()) {
            long oldMaxTimestamp = workflowScheduler.getMaxTimestampPerHost().get(hostName);
            long newMaxTimestamp = oldMaxTimestamp;
            for (long taskId : workflowScheduler.getTaskIds()) {
                // WorkflowDriver.writeToStdout("HiwayDB: Querying InvocStats for task id " + taskId + " on host " + hostName + " since timestamp " + oldMaxTimestamp
                // + " from database.");
                Collection<InvocStat> invocStats = dbInterface.getLogEntriesForTaskOnHostSince(taskId, hostName, oldMaxTimestamp);
                // WorkflowDriver.writeToStdout("HiwayDB: Retrieved InvocStats " + invocStats.toString() + " from database.");
                for (InvocStat stat : invocStats) {
                    newMaxTimestamp = Math.max(newMaxTimestamp, stat.getTimestamp());
                    workflowScheduler.updateRuntimeEstimate(stat);
                    if (!runId.equals(stat.getRunId())) {
                        workflowScheduler.setNumberOfPreviousRunTasks(workflowScheduler.getNumberOfPreviousRunTasks() + 1);
                        workflowScheduler.setNumberOfFinishedTasks(workflowScheduler.getNumberOfFinishedTasks() + 1);
                    }
                }
            }
            workflowScheduler.getMaxTimestampPerHost().put(hostName, newMaxTimestamp);
        }
    }

    public void updateRuntimeEstimate(InvocStat stat) {
        // for (FileStat fileStat : stat.getInputFiles()) {
        // re.timeSpent += fileStat.getRealTime();
        // }
        // for (FileStat fileStat : stat.getOutputFiles()) {
        // re.timeSpent += fileStat.getRealTime();
        // }
        Estimate.RuntimeEstimate re = runtimeEstimatesPerNode.get(stat.getHostName()).get(stat.getTaskId());
        re.finishedTasks += 1;
        re.timeSpent += stat.getRealTime();
        // for (FileStat fileStat : stat.getInputFiles()) {
        // re.timeSpent += fileStat.getRealTime();
        // }
        // for (FileStat fileStat : stat.getOutputFiles()) {
        // re.timeSpent += fileStat.getRealTime();
        // }
        re.weight = re.averageRuntime = re.timeSpent / re.finishedTasks;
    }

    public void setHdfs(FileSystem hdfs) {
        this.hdfs = hdfs;
    }

    private FileSystem getHdfs() {
        return hdfs;
    }
}