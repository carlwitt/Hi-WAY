package de.huberlin.wbi.hiway.scheduler;

import org.apache.hadoop.yarn.api.records.Priority;

import static org.junit.Assert.*;

/**
 * Created by Carl Witt on 02.11.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class WorkflowSchedulerTest {

    /** Confirms that lower values correspond to higher priorities, e.g., for container requests. */
    @org.junit.Test
    public void priorityTest(){
        Priority lowP = Priority.newInstance(100);
        Priority highP = Priority.newInstance(2);
        Integer low = 0;
        Integer high = 100;
        System.out.println("low.compareTo(high) = " + low.compareTo(high));
        System.out.println("high.compareTo(low) = " + high.compareTo(low));
        System.out.println("lowP.compareTo(highP) = " + lowP.compareTo(highP));
        System.out.println("highP.compareTo(lowP) = " + highP.compareTo(lowP));
    }

}