package de.huberlin.wbi.hiway.am;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		// these messages are only received when requested via 	am.getNmClientAsync().getContainerStatusAsync(containerId, container.getNodeId());
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
