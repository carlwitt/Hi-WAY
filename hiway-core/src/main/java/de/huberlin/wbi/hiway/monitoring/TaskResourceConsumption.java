/******************************************************************************
 In the Hi-WAY project we propose a novel approach of executing scientific
 workflows processing Big Data, as found in NGS applications, on distributed
 computational infrastructures. The Hi-WAY software stack comprises the func-
 tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 for Apache Hadoop 2.x (YARN).

 List of Contributors:

 Marc Bux (HU Berlin)
 Jörgen Brandt (HU Berlin)
 Hannes Schuh (HU Berlin)
 Carl Witt (HU Berlin)
 Ulf Leser (HU Berlin)

 Jörgen Brandt is funded by the European Commission through the BiobankCloud
 project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 research training group SOAMED (GRK 1651).

 Copyright 2014 Humboldt-Universität zu Berlin

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package de.huberlin.wbi.hiway.monitoring;

/**
 * Encapsulates data about the resource usage of a task.
 *
 *  Created by Carl Witt on 13.10.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class TaskResourceConsumption {

    /** The maximum memory consumed by the task (bytes). */
    private long memoryByteMax;
    /** The median memory consumed by the task (bytes). */
    private long memoryByteFifty;
    /** The .9 percentile of memory consumed by the task (bytes). */
    private long memoryByteNinety;
    /** The .95 percentile of memory consumed by the task (bytes). */
    private long memoryByteNinetyFive;


    public TaskResourceConsumption() { }

    public TaskResourceConsumption(long memoryByteMax, long memoryByteFifty, long memoryByteNinety, long memoryByteNinetyFive) {
        this.memoryByteMax = memoryByteMax;
        this.memoryByteFifty = memoryByteFifty;
        this.memoryByteNinety = memoryByteNinety;
        this.memoryByteNinetyFive = memoryByteNinetyFive;
    }

    @Override
    public String toString() {
        return "TaskResourceConsumption{" +
                "memory Max (MiB) = " + memoryByteMax/1024/1024 +
                ", memory Fifty (MiB) = " + memoryByteFifty/1024/1024 +
                ", memory Ninety (MiB) = " + memoryByteNinety/1024/1024 +
                ", memory NinetyFive (MiB) = " + memoryByteNinetyFive/1024/1024 +
                '}';
    }

    /** A setter that updates the value only if it is smaller than the proposed value. */
    public void proposeMemoryByteMax(long proposedMemoryByteMax) {
        memoryByteMax = Math.max(memoryByteMax, proposedMemoryByteMax);
    }

    public long getMemoryByteMax() {
        return memoryByteMax;
    }

    public void setMemoryByteMax(long memoryByteMax) {
        this.memoryByteMax = memoryByteMax;
    }

    public long getMemoryByteFifty() {
        return memoryByteFifty;
    }

    public void setMemoryByteFifty(long memoryByteFifty) {
        this.memoryByteFifty = memoryByteFifty;
    }

    public long getMemoryByteNinety() {
        return memoryByteNinety;
    }

    public void setMemoryByteNinety(long memoryByteNinety) {
        this.memoryByteNinety = memoryByteNinety;
    }

    public long getMemoryByteNinetyFive() {
        return memoryByteNinetyFive;
    }

    public void setMemoryByteNinetyFive(long memoryByteNinetyFive) {
        this.memoryByteNinetyFive = memoryByteNinetyFive;
    }
}
