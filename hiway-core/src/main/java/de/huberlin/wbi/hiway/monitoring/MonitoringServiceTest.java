package de.huberlin.wbi.hiway.monitoring;


import java.lang.management.ManagementFactory;

/**
 * Created by Carl Witt on 31.07.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class MonitoringServiceTest {

    public static void main(String[] args) {
        /* Total number of processors or cores available to the JVM */
        System.out.println("Available processors (cores): " +
                Runtime.getRuntime().availableProcessors());

        /* Total amount of free memory available to the JVM */
        System.out.println("Free memory (MB): " +
                Runtime.getRuntime().freeMemory()/1024/1024);

        /* This will return Long.MAX_VALUE if there is no preset limit */
        long maxMemory = Runtime.getRuntime().maxMemory();
        /* Maximum amount of memory the JVM will attempt to use */
        System.out.println("Maximum memory (MB): " +
                (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory/1024/1024));

        /* Total memory currently available to the JVM */
        System.out.println("Total memory available to JVM (MB): " +
                Runtime.getRuntime().totalMemory()/1024/1024);

        //The java.lang.management package does give you a whole lot more info than Runtime - for example it will give you heap memory (
        System.out.println("Bean heap memory usage: " + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
        System.out.println("Bean NON-heap memory usage: " + ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());

        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long physicalMemorySize = os.getTotalPhysicalMemorySize();
        long freePhysicalMemory = os.getFreePhysicalMemorySize();
        long freeSwapSize = os.getFreeSwapSpaceSize();
        long commitedVirtualMemorySize = os.getCommittedVirtualMemorySize();

        System.out.println("physicalMemorySize/G = " + physicalMemorySize/1024/1024);
        System.out.println("freePhysicalMemory/G = " + freePhysicalMemory/1024/1024);
        System.out.println("freeSwapSize/G = " + freeSwapSize/1024/1024);
        System.out.println("commitedVirtualMemorySize/G = " + commitedVirtualMemorySize/1024/1024);


    }
}