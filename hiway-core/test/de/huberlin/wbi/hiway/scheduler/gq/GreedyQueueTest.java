package de.huberlin.wbi.hiway.scheduler.gq;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.am.dax.DaxTaskInstance;
import de.huberlin.wbi.hiway.common.TaskInstance;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by Carl Witt on 11/24/17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class GreedyQueueTest {

    static GreedyQueue scheduler = new GreedyQueue("test-workflow");
    static UUID workflowId = UUID.randomUUID();

    static YarnProtos.ContainerProto containerProto = YarnProtos.ContainerProto.getDefaultInstance();
    static ContainerPBImpl container = new ContainerPBImpl(containerProto);

    // cluster ID
    static long clusterTimestamp = 1L;

    // application ID
    static int applicationID = 120;
    static int attemptId = 1;
    static ApplicationId appId = ApplicationId.newInstance(clusterTimestamp, applicationID);
    static ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, attemptId);

    // container ID
    static long containerId = 1029L;

    // node ID
    static{
        container.setId(ContainerId.newContainerId(appAttemptId, containerId));
        System.out.println("container.getId() = " + container.getId());
        container.setNodeId(NodeId.newInstance("simulatedHost",8080));
        System.out.println("container.getNodeId() = " + container.getNodeId());
        System.out.println("container.getNodeId().getHost() = " + container.getNodeId().getHost());
    }

    /**
     * Test, whether using the scheduler in CloudSim (i.e., separated from the distributed runtime environment) works.
     * Looks good, needs a marshalling component that translates between Hi-WAY, YARN, and CloudSim objects:
     * - DynamicCloudSim intermingles Tasks and Cloudlets, while Hi-WAY has its own workflow data structures
     * - YARN resource requests can be used as is and translated from a YARN-Sim component to CloudSim resources and back to YARN Containers
     */
    @Test
    public void scheduleTaskToContainer() throws Exception {
        DaxTaskInstance task = new DaxTaskInstance(workflowId, "split");
        scheduler.addTask(task);
        scheduler.enqueueResourceRequest(task);
        TaskInstance t = scheduler.scheduleTaskToContainer(container);
        System.out.println("t = " + t);
    }



}