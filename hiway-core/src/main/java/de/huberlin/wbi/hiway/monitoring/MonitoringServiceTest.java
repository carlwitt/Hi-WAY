package de.huberlin.wbi.hiway.monitoring;


import java.lang.management.ManagementFactory;

/**
 * Created by Carl Witt on 31.07.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
class MonitoringServiceTest {

    /**
     * example HTTP response from http://dbis71:22223/api/v2.0/stats/the_container_name?type=docker&count=1
     * {"/docker/06acddec23bbc978fb854289c539fd2804eb96497fb1611fcaf6385b13b0411c":[{"timestamp":"2017-09-27T12:18:58.447587743Z","has_cpu":true,"cpu":{"usage":{"total":486644704,"per_cpu_usage":[0,131128,211707,0,0,0,463906121,823493,16350440,1884095,729259,267583,0,0,0,0,0,0,753626,0,0,0,1396778,190474],"user":0,"system":480000000},"cfs":{"periods":0,"throttled_periods":0,"throttled_time":0},"load_average":0},"has_diskio":true,"diskio":{},"has_memory":true,"memory":{"usage":999411712,"cache":999309312,"rss":102400,"swap":0,"working_set":999411712,"failcnt":0,"container_data":{"pgfault":617,"pgmajfault":0},"hierarchical_data":{"pgfault":617,"pgmajfault":0}},"has_network":true,"network":{"interfaces":[{"name":"eth0","rx_bytes":648,"rx_packets":8,"rx_errors":0,"rx_dropped":0,"tx_bytes":0,"tx_packets":0,"tx_errors":0,"tx_dropped":0}],"tcp":{"Established":0,"SynSent":0,"SynRecv":0,"FinWait1":0,"FinWait2":0,"TimeWait":0,"Close":0,"CloseWait":0,"LastAck":0,"Listen":0,"Closing":0},"tcp6":{"Established":0,"SynSent":0,"SynRecv":0,"FinWait1":0,"FinWait2":0,"TimeWait":0,"Close":0,"CloseWait":0,"LastAck":0,"Listen":0,"Closing":0}},"has_filesystem":true,"filesystem":[{"device":"/dev/sda1","type":"vfs","capacity":950583918592,"usage":0,"base_usage":12288,"available":0,"has_inodes":false,"inodes":0,"inodes_free":0,"reads_completed":0,"reads_merged":0,"sectors_read":0,"read_time":0,"writes_completed":0,"writes_merged":0,"sectors_written":0,"write_time":0,"io_in_progress":0,"io_time":0,"weighted_io_time":0}],"has_load":false,"load_stats":{"nr_sleeping":0,"nr_running":0,"nr_stopped":0,"nr_uninterruptible":0,"nr_io_wait":0},"has_custom_metrics":false}]}
     */

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