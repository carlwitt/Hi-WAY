package de.huberlin.wbi.hiway.am.dax;

import de.huberlin.wbi.hiway.common.Data;

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
        d.setPeakMemoryConsumption(1000);
        d.setRuntimeSeconds(3);
        d.addOutputData(new Data("/local/path/file.txt"), 12345L);
        System.out.println("d.getCommand() = " + d.getCommand());
    }

}