package de.huberlin.wbi.hiway.am;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.http.client.HttpResponseException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The application master's communication device to all the node managers.
 * Receives messages about container start, stop, status, and error messages.
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	private final WorkflowDriver am;
	private final ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();


	NMCallbackHandler(WorkflowDriver am) {
		this.am = am;
	}

	void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		Container container = containers.get(containerId);
		if (container != null) {
			am.getNmClientAsync().getContainerStatusAsync(containerId, container.getNodeId());
		}
	}

	/**
	 * Upon receiving container status, poll the cadvisor for resource usage information.
	 */
	@Override
	public void onContainerStatusReceived(ContainerId cId, ContainerStatus cStatus) {
		/* log */ if (HiWayConfiguration.debug) WorkflowDriver.writeToStdout(String.format("Container %s status %s",containers.get(cId).getNodeHttpAddress(),cStatus));

		// pull the resource usage only if the container is reported as running (it might have stopped in the meantime,
		// but at least no early requests are performed and I don't need to issue periodic polls myself).
		if(cStatus.getState().equals(ContainerState.RUNNING)){
			try {
				JSONObject stats = pollContainerStatsRemote(containers.get(cId));
//				/* log */ if(HiWayConfiguration.debug)
				WorkflowDriver.writeToStdout(String.format("cadvisor %s status %s",containers.get(cId).getNodeHttpAddress(),stats.toString()));
			} catch (HttpResponseException e) {
				e.printStackTrace();
				/* log */ WorkflowDriver.writeToStdout(String.format("Received a %s HTTP Response from cadvisor endpoint at Node %s",e.getStatusCode(), containers.get(cId).getNodeHttpAddress()));
			} catch (Exception e){
				e.printStackTrace();
				/* log */ WorkflowDriver.writeToStdout(String.format("Observed %s when trying to poll cadvisor endpoint at Node %s", e.getMessage(), containers.get(cId).getNodeHttpAddress()));
			}

		}
	}

	/**
	 * Connects to the remote cadvisor service via REST to poll memory usage information for the container.
	 * Relies on the container's name being equal to the {@link TaskInstance#getDockerContainerName()}.
	 * The cadvisor port is specified through {@link HiWayConfiguration#HIWAY_MONITORING_CADVISOR_PORT}.
	 * @return the JSON data delivered by cadvisor, empty JSON Object if something goes wrong
	 */
	private JSONObject pollContainerStatsRemote(Container container) throws /* HttpResponseException extends */IOException, JSONException {
		try{

			// assemble the cAdvisor endpoint from the Hi-WAY configuration
			String cadvisorPort = am.getConf().get(HiWayConfiguration.HIWAY_MONITORING_CADVISOR_PORT);
			String hostName = containers.get(container.getId()).getNodeHttpAddress().split(":")[0];
			String restCommand = String.format("api/v2.0/stats/%s?type=docker&count=1",am.taskInstanceByContainer.get(container).getDockerContainerName());
			// e.g., http://dbis61:22223/api/v2.0/stats/a6c6f4c5-cbde-4510-82a4-f4f3c7c97bc4_3?type=docker&count=1
			URL cadvisorEndpoint = new URL(String.format("http://%s:%s/%s", hostName, cadvisorPort, restCommand));

			// open a HTTP connection
			HttpURLConnection conn = (HttpURLConnection) cadvisorEndpoint.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			// if this returns a 500, the container has probably been stopped already
			if(conn.getResponseCode() != 200){
				throw new HttpResponseException(conn.getResponseCode(), " No valid response from cadvisor at " + cadvisorEndpoint.toString());
			}

			// read the contents from the input stream
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			StringBuilder buf = new StringBuilder();
			while ((output = br.readLine()) != null) {
				buf.append(output);
			}

			conn.disconnect();
			return new JSONObject(buf.toString());

		} catch(IOException | JSONException e){
			/* log */ e.printStackTrace();
			/* log */ WorkflowDriver.writeToStdout("ERROR" + e.getMessage());
			throw e;
		}
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		containers.remove(containerId);
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		/* log */ WorkflowDriver.writeToStdout("Failed to query the status of Container " + containerId);
		/* log */ t.printStackTrace();
		System.exit(-1);
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		/* log */ WorkflowDriver.writeToStdout("Failed to start Container " + containerId);
		/* log */ t.printStackTrace();
		containers.remove(containerId);
		am.getNumCompletedContainers().incrementAndGet();
		am.getNumFailedContainers().incrementAndGet();
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		/* log */ WorkflowDriver.writeToStdout("Failed to stop Container " + containerId);
		/* log */ t.printStackTrace();
		containers.remove(containerId);
	}
}
