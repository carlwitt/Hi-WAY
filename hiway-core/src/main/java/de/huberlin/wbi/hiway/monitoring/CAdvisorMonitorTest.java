package de.huberlin.wbi.hiway.monitoring;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by Carl Witt on 12.10.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class CAdvisorMonitorTest {
    @Test
    public void startMonitoring() throws Exception {
        CAdvisorMonitor monitor = new CAdvisorMonitor("dbis64:22223", "cadvisor", new File("cadvisor.txt"));
        monitor.startMonitoring();
        System.out.println("started monitoring...");
        Thread.sleep(4000);
        monitor.stopMonitoring();
    }

}