/*
 In the Hi-WAY project we propose a novel approach of executing scientific
 workflows processing Big Data, as found in NGS applications, on distributed
 computational infrastructures. The Hi-WAY software stack comprises the func-
 tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 for Apache Hadoop 2.x (YARN).

 List of Contributors:

 Marc Bux (HU Berlin)
 Jörgen Brandt (HU Berlin)
 Hannes Schuh (HU Berlin)
 Carl Witt (HU Berlin)
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
package de.huberlin.wbi.hiway.monitoring;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * For use in a thread that polls cadvisor for resource usage metrics of a container.<br/>
 *
 * Uses the container stats feature to retrieve the data in JSON format<br/>
 *      http://{host}:{cadvisor-port}/api/v2.0/stats/{container-name}type=docker&count=1<br/>
 *
 * A baseline approach would be to execute a "top" command in the container:<br/>
 *       new ProcessBuilder().inheritIO().command("docker", "exec", dockerContainerName, "head", "-6", "/proc/meminfo").start();<br/>
 *
 * An interesting an (theoretically) useful feature of cadvisor is to get aggregate container statistics at<br/>
 *      http://{host}:8080/api/v2.0/summary/{container-name}?type=docker<br/>
 * However, this is only useful if the container runs longer than a minute, otherwise all values (except latest_usage and timestamp) are zero.<br/>
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class CAdvisorMonitor implements Runnable {

    /** the task resource usage observed since the start of monitoring (i.e., should be the same as the container lifetime) */
    private TaskResourceConsumption taskResourceConsumption = new TaskResourceConsumption();

    /** The URL to the cAdvisor endpoint */
    URL url;

    /** The thread used to run and interrupt the polling loop. */
    private Thread thread;
    /** The time between consecutive metric fetches. */
    private long pollingDelay = 1000;

    /** Whether to write the observed metrics to a file. */
    private boolean writeToFile;
    /** The file to write observations (in JSON format) to. Only relevant if {@link #writeToFile} is true. */
    private File metricsFile;

    /** Just for logging */
    private String dockerContainerNameShort;

    public CAdvisorMonitor(String cAdvisorHostAndPort, String dockerContainerName) {
        this(cAdvisorHostAndPort, dockerContainerName, null);
        writeToFile = false;
    }
    /**
     * @param cAdvisorHostAndPort for example dbis64:22223 or localhost:8080
     * @param dockerContainerName the name given to the docker container, e.g., via the --name flag
     * @param metricsFile where to write the observations
     */
    CAdvisorMonitor(String cAdvisorHostAndPort, String dockerContainerName, File metricsFile) {

        writeToFile = true;
        this.metricsFile = metricsFile;

        String apiEndpoint = String.format(getContainerSummaryStatsTemplate, cAdvisorHostAndPort, dockerContainerName);

        try {
            url = new URL(apiEndpoint);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        dockerContainerNameShort = dockerContainerName.substring(0, 8);
    }

    /** REST command to get aggregated (percentiles) resource usage statistics of a container. e.g., {"/docker/58a032ee71c1030b5f0012ed2d12fc62cf3ac540013e76780e316c253971ce24":{"timestamp":"2017-10-12T14:49:27.186757129Z","latest_usage":{"cpu":19,"memory":39944192},"minute_usage":{"percent_complete":100,"cpu":{"present":true,"mean":29,"max":107,"fifty":25,"ninety":47,"ninetyfive":57},"memory":{"present":true,"mean":40017920,"max":40017920,"fifty":40017920,"ninety":40017920,"ninetyfive":40017920}},"hour_usage":{"percent_complete":100,"cpu":{"present":true,"mean":28,"max":147,"fifty":44,"ninety":49,"ninetyfive":50},"memory":{"present":true,"mean":42015726,"max":59813888,"fifty":40640512,"ninety":48585523,"ninetyfive":51488153}},"day_usage":{"percent_complete":4,"cpu":{"present":true,"mean":28,"max":147,"fifty":44,"ninety":49,"ninetyfive":50},"memory":{"present":true,"mean":42015726,"max":59813888,"fifty":40640512,"ninety":48585523,"ninetyfive":51488153}}}}*/
    private static final String getContainerSummaryStatsTemplate = "http://%s/api/v2.0/summary/%s?type=docker";
    /** REST command to get resource usage statistics (time series) of a container, e.g., {"has_diskio":true,"has_memory":true,"load_stats":{"nr_uninterruptible":0,"nr_sleeping":0,"nr_stopped":0,"nr_io_wait":0,"nr_running":0},"cpu":{"load_average":0,"usage":{"total":2831301951490,"system":1774380000000,"per_cpu_usage":[64589902363,63998677036,...],"user":901590000000},"cfs":{"throttled_time":0,"periods":0,"throttled_periods":0}},"has_network":true,"diskio":{"io_service_bytes":[{"minor":0,"stats":{"Async":15556608,"Sync":0,"Write":0,"Read":15556608,"Total":15556608},"major":8}],"io_serviced":[{"minor":0,"stats":{"Async":183,"Sync":0,"Write":0,"Read":183,"Total":183},"major":8}]},"memory":{"failcnt":0,"cache":15593472,"container_data":{"pgmajfault":131,"pgfault":1195017},"hierarchical_data":{"pgmajfault":131,"pgfault":1195017},"rss":40988672,"usage":56877056,"swap":0,"working_set":42455040},"network":{"interfaces":[{"tx_errors":0,"tx_bytes":13560702,"rx_dropped":0,"tx_packets":2858,"rx_packets":5671,"name":"eth0","tx_dropped":0,"rx_errors":0,"rx_bytes":606450}],"tcp6":{"Listen":0,"SynRecv":0,"Close":0,"Closing":0,"LastAck":0,"TimeWait":0,"SynSent":0,"Established":0,"CloseWait":0,"FinWait1":0,"FinWait2":0},"tcp":{"Listen":0,"SynRecv":0,"Close":0,"Closing":0,"LastAck":0,"TimeWait":0,"SynSent":0,"Established":0,"CloseWait":0,"FinWait1":0,"FinWait2":0}},"timestamp":"2017-10-12T14:19:50.69763586Z","has_cpu":true,"has_custom_metrics":false,"has_filesystem":true,"filesystem":[{"inodes_free":0,"writes_merged":0,"io_in_progress":0,"reads_completed":0,"base_usage":40960,"available":0,"type":"vfs","read_time":0,"weighted_io_time":0,"inodes":0,"io_time":0,"has_inodes":false,"device":"/dev/sda1","writes_completed":0,"capacity":959039111168,"usage":0,"write_time":0,"reads_merged":0,"sectors_written":0,"sectors_read":0}],"has_load":false} */
    private static final String getContainerStatsTemplate = "http://%s/api/v2.0/stats/%s?type=docker&count=1";
    /** REST command to retrieve hardware and software information about a host. */
    private static final String getMachineStatsTemplate = "http://%s/api/v2.0/machine";

    /** Spawns and starts the monitoring thread. This is a non-blocking operation.
     * TODO: for very many concurrent tasks, it might be better to have one monitoring thread that accepts hosts and container names and bundles the polling work, instead of having thousands of polling threads. Maybe there's a way to poll several containers from a single host. */
    public void startMonitoring(){
        thread = new Thread(this);
        thread.start();
    }
    /** Quits the polling of the cadvisor. */
    public void stopMonitoring(){
        thread.interrupt();
    }

    @Override
    public void run() {

        FileWriter metricsFileWriter = null;
        if(writeToFile){
            try {
                metricsFileWriter = new FileWriter(metricsFile,true);
            } catch (IOException e) {
                Logger.getLogger("cAdvisorMonitor").log(Level.SEVERE, "Could not open metrics output file. "+metricsFile);
                e.printStackTrace();
                return;
            }
        }

        try {

            // to discard duplicate responses (with the same timestamp)
            int lastTimeStampHash = -1;

            while (!Thread.currentThread().isInterrupted()) {

                try {

                    Thread.sleep(pollingDelay);

                    // fetch HTTP response from REST API
                    JSONObject stats = httpGetAndParse();
                    if (stats == null) continue;

                    // unravel returned data
                    String dockerInternalContainerName = (String) stats.keys().next();
                    // use stats.getJSONArray(dockerInternalContainerName).getJSONObject(0); for {@link #getContainerStatsTemplate}
                    JSONObject data = stats.getJSONObject(dockerInternalContainerName);

                    // process only if not already seen
                    int currentTimeStampHash = data.getString("timestamp").hashCode();

                    // compute peak memory usage (container stats (non-summary) version)
//                    long currentMemoryUsage = data.getJSONObject("memory").getLong("usage");
//                    memoryByteMax = Math.max(memoryByteMax, currentMemoryUsage);
                    // these values can be zero if the container hasn't been running for a long time
                    // in addition, compute the maximum over the entire lifespan (a container could run longer than a day)
                    long p = data.getJSONObject("day_usage").getJSONObject("memory").getLong("max");
                    if (p == 0) p = data.getJSONObject("hour_usage").getJSONObject("memory").getLong("max");
                    if (p == 0) p = data.getJSONObject("minute_usage").getJSONObject("memory").getLong("max");
                    if (p == 0) p = data.getJSONObject("latest_usage").getLong("memory");
                    taskResourceConsumption.proposeMemoryByteMax(p);
                    taskResourceConsumption.setMemoryByteFifty(data.getJSONObject("day_usage").getJSONObject("memory").getLong("fifty"));
                    taskResourceConsumption.setMemoryByteNinety(data.getJSONObject("day_usage").getJSONObject("memory").getLong("ninety"));
                    taskResourceConsumption.setMemoryByteNinetyFive(data.getJSONObject("day_usage").getJSONObject("memory").getLong("ninetyfive"));

                    if (currentTimeStampHash != lastTimeStampHash) {
                        if (writeToFile) {
                            metricsFileWriter.write(data.toString() + "\n");
                            metricsFileWriter.flush();
                        }
                    }

                    lastTimeStampHash = currentTimeStampHash;

                } catch (JSONException | IOException e ) {
                    /* log */ Logger.getLogger("cAdvisorMonitor").log(Level.WARNING, e.getMessage());
                } catch (InterruptedException e){
                    // this occurs when the interrupt is received during Thread.sleep -- no special action required here.
                }
            } // while ! interrupted
        } finally {
            try {
                if(writeToFile){
                    metricsFileWriter.write("]");
                    metricsFileWriter.close();
                }
            } catch (IOException e) {
                /* log */ Logger.getLogger("cAdvisorMonitor").log(Level.SEVERE, "Could not close metrics output file. " );
                /* log */ e.printStackTrace();
            }
        }
    }

    /** Makes the HTTP request to the cadvisor endpoint and returns the parsed response
     * Returns null when getting response code 500 */
    private JSONObject httpGetAndParse()  {

        try {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            // if this returns a 500, the container has been already stopped
            if(conn.getResponseCode() == 500){
                return null;
            } else if (conn.getResponseCode() != 200) {
                throw new IOException("Failed : HTTP error code " + conn.getResponseCode());
            }

            // read response
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            String output;
            StringBuilder buf = new StringBuilder();
            while ((output = br.readLine()) != null) buf.append(output);

            conn.disconnect();
            return new JSONObject(buf.toString());

        } catch (IOException | JSONException e) { e.printStackTrace(); }

        return new JSONObject();
    }

    public void setPollingDelay(long milliseconds){
        pollingDelay = milliseconds;
    }

    Thread getThread() {
        return thread;
    }

    public TaskResourceConsumption getTaskResourceConsumption() { return taskResourceConsumption; }

//    /**
//     * Connects to the remote cadvisor service via REST to poll memory usage information for the container.
//     * Relies on the container's name being equal to the {@link TaskInstance#getDockerContainerName()}.
//     * The cadvisor port is specified through {@link HiWayConfiguration#HIWAY_MONITORING_CADVISOR_PORT}.
//     * @return the JSON data delivered by cadvisor, empty JSON Object if something goes wrong
//     */
//    private JSONObject pollContainerStatsRemote(Container container) throws /* HttpResponseException extends */IOException, JSONException {
//        try{
//
//            // assemble the cAdvisor endpoint from the Hi-WAY configuration
//            String cadvisorPort = am.getConf().get(HiWayConfiguration.HIWAY_MONITORING_CADVISOR_PORT);
//            String hostName = containers.get(container.getId()).getNodeHttpAddress().split(":")[0];
//            String restCommand = String.format("api/v2.0/stats/%s?type=docker&count=1",am.taskInstanceByContainer.get(container).getDockerContainerName());
//            // e.g., http://dbis61:22223/api/v2.0/stats/a6c6f4c5-cbde-4510-82a4-f4f3c7c97bc4_3?type=docker&count=1
//            URL cadvisorEndpoint = new URL(String.format("http://%s:%s/%s", hostName, cadvisorPort, restCommand));
//
//            // open a HTTP connection
//            HttpURLConnection conn = (HttpURLConnection) cadvisorEndpoint.openConnection();
//            conn.setRequestMethod("GET");
//            conn.setRequestProperty("Accept", "application/json");
//
//            // if this returns a 500, the container has probably been stopped already
//            if(conn.getResponseCode() != 200){
//                throw new HttpResponseException(conn.getResponseCode(), " No valid response from cadvisor at " + cadvisorEndpoint.toString());
//            }
//
//            // read the contents from the input stream
//            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
//            String output;
//            StringBuilder buf = new StringBuilder();
//            while ((output = br.readLine()) != null) {
//                buf.append(output);
//            }
//
//            conn.disconnect();
//            return new JSONObject(buf.toString());
//
//        } catch(IOException | JSONException e){
//			/* log */ e.printStackTrace();
//			/* log */ WorkflowDriver.writeToStdout("ERROR" + e.getMessage());
//            throw e;
//        }
//    }

//    /**
//     * Upon receiving container status, poll the cadvisor for resource usage information.
//     */
//    @Override
//    public void onContainerStatusReceived(ContainerId cId, ContainerStatus cStatus) {
//		/* log */ if (HiWayConfiguration.debug) WorkflowDriver.writeToStdout(String.format("Container %s status %s",containers.get(cId).getNodeHttpAddress(),cStatus));
//
//        // pull the resource usage only if the container is reported as running (it might have stopped in the meantime,
//        // but at least no early requests are performed and I don't need to issue periodic polls myself).
//        if(cStatus.getState().equals(ContainerState.RUNNING)){
//            try {
//                JSONObject stats = pollContainerStatsRemote(containers.get(cId));
//				/* log */ WorkflowDriver.writeToStdout(String.format("cadvisor %s status %s",containers.get(cId).getNodeHttpAddress(),stats.toString()));
//            } catch (HttpResponseException e) {
//                e.printStackTrace();
//				/* log */ WorkflowDriver.writeToStdout(String.format("Received a %s HTTP Response from cadvisor endpoint at Node %s",e.getStatusCode(), containers.get(cId).getNodeHttpAddress()));
//            } catch (Exception e){
//                e.printStackTrace();
//				/* log */ WorkflowDriver.writeToStdout(String.format("Observed %s when trying to poll cadvisor endpoint at Node %s", e.getMessage(), containers.get(cId).getNodeHttpAddress()));
//            }
//
//        }
//    }
}
