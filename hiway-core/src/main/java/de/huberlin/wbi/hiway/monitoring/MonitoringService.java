/*
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

import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;

import java.nio.ByteBuffer;

/**
 * Created by Carl Witt on 31.07.17.
 *
 * An auxiliary service measuring cross-container memory usage.
 *
 * Based on the blog post http://johnjianfang.blogspot.de/2014/09/auxiliaryservice-in-hadoop-2.html
 *
 * @author Carl Witt (cpw@posteo.de)
 */
class MonitoringService extends org.apache.hadoop.yarn.server.api.AuxiliaryService{


    protected MonitoringService(String name) {
        super(name);
    }

    @Override
    public void initializeApplication(ApplicationInitializationContext applicationInitializationContext) {

    }

    @Override
    public void stopApplication(ApplicationTerminationContext applicationTerminationContext) {

    }

    @Override
    public ByteBuffer getMetaData() {
        return null;
    }
}