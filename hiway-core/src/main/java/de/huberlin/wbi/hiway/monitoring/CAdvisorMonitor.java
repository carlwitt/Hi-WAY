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
 * Carl Witt (HU Berlin)
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
package de.huberlin.wbi.hiway.monitoring;

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

    /** The URL to the cAdvisor endpoint */
    URL url;
    /** Just for logging */
    private String dockerContainerNameShort;
    private File metricsFile;
    /** The time between consecutive metric fetches. */
    private int pollingDelay = 1000;
    /** Flag to indicate that polling should stop. */
    private volatile boolean stopped = false;

    CAdvisorMonitor(String cAdvisorHostAndPort, String dockerContainerName, File metricsFile) {

        dockerContainerNameShort = dockerContainerName.substring(0, 8);
        this.metricsFile = metricsFile;
        String apiEndpoint = String.format("http://%s/api/v2.0/stats/%s?type=docker&count=1", cAdvisorHostAndPort, dockerContainerName);

        try {
            url = new URL(apiEndpoint);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    /** Signals the end of the monitoring loop, which results in flushing the file buffers. */
    void stopAndWriteOut(){
        this.stopped = true;
    }

    @Override
    public void run() {

        boolean firstEntry = true;
        FileWriter metricsFileWriter = null;
        try {
            metricsFileWriter = new FileWriter(metricsFile,true);
            metricsFileWriter.write("[");

        } catch (IOException e) {
            Logger.getLogger("cAdvisorMonitor").log(Level.SEVERE, "Could not open metrics output file. "+metricsFile);
            e.printStackTrace();
        }

        long peakMemoryBytes = 0;

        try {
            int lastTimeStampHash = -1;
            while (!stopped){   //Thread.currentThread().isInterrupted()

                Thread.sleep(pollingDelay);

                JSONObject stats;
                try {

                    stats = getStatsFromCAdvisor();
                    if(stats == null) continue;

                    String dockerInternalContainerName = (String) stats.keys().next();
                    JSONObject data = stats.getJSONArray(dockerInternalContainerName).getJSONObject(0);

                    // process only if not already seen
                    int currentTimeStampHash = data.getString("timestamp").hashCode();
                    long currentMemoryUsage = data.getJSONObject("memory").getLong("usage");
                    peakMemoryBytes = Math.max(peakMemoryBytes, currentMemoryUsage);

                    if(currentTimeStampHash != lastTimeStampHash){

                        if(!firstEntry) metricsFileWriter.write(",");

                        metricsFileWriter.write(data.toString());
                        metricsFileWriter.write("\n");

                        firstEntry = false;
                    }

                    lastTimeStampHash = currentTimeStampHash;

                } catch (Exception e) {
                    Logger.getLogger("cAdvisorMonitor").log(Level.WARNING, e.getMessage());
                }

            }
        } catch (InterruptedException e) {
            Logger.getLogger("cAdvisorMonitor").log(Level.INFO, dockerContainerNameShort + " monitor stopped.");
        } finally {
            System.out.println(dockerContainerNameShort + " usage [GB]" + 1.0*peakMemoryBytes/1000/1000/1000);

            try {
                metricsFileWriter.write("]");
                metricsFileWriter.close();
            } catch (IOException e) {
                Logger.getLogger("cAdvisorMonitor").log(Level.SEVERE, "Could not close metrics output file. " );
                e.printStackTrace();
            }
        }
    }

    /** Makes the HTTP request to the cadvisor endpoint and returns the parsed response
     * Returns null when getting response code 500 */
    private JSONObject getStatsFromCAdvisor() throws Exception {

        try {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            // if this returns a 500, the container has been already stopped
            if(conn.getResponseCode() == 500){
                this.stopped = true;
                return null;
            } else if (conn.getResponseCode() != 200) {
                throw new Exception("Failed : HTTP error code " + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;

            StringBuffer buf = new StringBuffer();
            while ((output = br.readLine()) != null) {
                buf.append(output);
            }

            conn.disconnect();
            return new JSONObject(buf.toString());

        } catch (IOException e) {

            e.printStackTrace();

        }

        return new JSONObject();
    }
}
