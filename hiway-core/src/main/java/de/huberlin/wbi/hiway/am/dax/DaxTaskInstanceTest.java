package de.huberlin.wbi.hiway.am.dax;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import org.apache.hadoop.yarn.api.records.Priority;

import java.util.Arrays;
import java.util.UUID;

/**
 * Created by Carl Witt on 10.08.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class DaxTaskInstanceTest {
    @org.junit.Test
    public void getCommand() throws Exception {

        DaxTaskInstance d = new DaxTaskInstance(new UUID(1,1), "");
        d.setContainerMemoryLimitBytes(4*1024*1024);
        d.setPeakMemoryConsumption(3*1024*1024);
        d.setRuntimeSeconds(5);
        d.addOutputData(new Data("output-split1.txt"), 12345L);
        d.addOutputData(new Data("output-merge.xml"), 10000L);
        System.out.println("d.getCommand() = " + d.getCommand());
    }

    @org.junit.Test
    public void multiplyBytes(){
        int containerMemMB = 5120;
        // silent overflow, because int ranges only up to 2^31-1 (i.e., 2G - 1) so calculating 5G returns exactly 1G (2^30)
        System.out.println(containerMemMB*1024*1024);
    }

    @org.junit.Test
    public void concatString(){
        String str = Arrays.toString(HiWayConfiguration.HIWAY_SCHEDULER_OPTS.values());
        System.out.println(str.substring(1,str.length()-1));
    }

    /** Confirms that lower values correspond to higher priorities, e.g., for container requests. */
    @org.junit.Test public void priorityTest(){
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