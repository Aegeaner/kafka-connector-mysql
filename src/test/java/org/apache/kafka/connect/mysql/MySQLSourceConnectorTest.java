package org.apache.kafka.connect.mysql;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MySQLSourceConnectorTest {

    private MySQLSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new MySQLSourceConnector();
        ctx = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();

        connector.start(sourceProperties);
        assertEquals(MySQLSourceTask.class, connector.taskClass());

        PowerMock.verifyAll();

    }

    @Test
    public void testStartStop() {
        PowerMock.replayAll();

        connector.start(sourceProperties);
        connector.stop();

        PowerMock.verifyAll();
    }
}
