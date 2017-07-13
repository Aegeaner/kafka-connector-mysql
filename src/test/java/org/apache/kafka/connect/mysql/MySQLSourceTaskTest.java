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


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MySQLSourceTaskTest {
    private static final long REPLICATOR_TIMEOUT = 5000;
    private static final long EVENT_BUFFER_POLL_TIMEOUT = 1;

    private static org.apache.kafka.connect.mysql.MySQLIsolatedServer server;
    private BinaryLogClient client;
    private org.apache.kafka.connect.mysql.MySQLBinlogEventListener eventListener;
    private org.apache.kafka.connect.mysql.MySQLBinlogEventBuffer eventBuffer;
    private HashMap<String, String> config;
    private org.apache.kafka.connect.mysql.MySQLSourceTask task;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private boolean verifyMocks = false;
    private org.apache.kafka.connect.mysql.EventRecordFactory eventRecordFactory;

    @BeforeClass
    public static void setUpDB() throws Exception {
        server = new MySQLIsolatedServer();
        server.boot();
    }

    @Before
    public void setUp() {
        Integer port = server.getPort();
        client = new BinaryLogClient("localhost", port, "replicator", "replicator");
        eventBuffer = new MySQLBinlogEventBuffer(3L, 0.95, "events");
        ArrayList<String> empty = new ArrayList<>();
        TopicConfig defaultTopicConf = new TopicConfig(empty, empty, empty, empty, "");
        RemoteConfig defaultRemoteConf = new RemoteConfig("localhost", String.valueOf(port), "replicator", "replicator");
        eventRecordFactory = new EventRecordFactory(defaultTopicConf, defaultRemoteConf);
        eventListener = new MySQLBinlogEventListener(client, "localhost", eventBuffer, null, eventRecordFactory);
        client.registerEventListener(eventListener);

        config = new HashMap<>();
        //config.put(MySQLSourceConfig.HOST_CONFIG, "127.0.0.1");
        config.put(MySQLSourceConfig.PORT_CONFIG, String.valueOf(server.getPort()));
        //config.put(MySQLSourceConfig.POLL_BATCH_SIZE, "4");
        //config.put(MySQLSourceConfig.USERNAME, "replicator");
        //config.put(MySQLSourceConfig.PASSWORD, "replicator");
        config.put(MySQLSourceConfig.IN_MEMORY_EVENT_SIZE, "3");
        //config.put(MySQLSourceConfig.MEMORY_RATIO, "0.5");

        task = new MySQLSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);
    }

    private void replay() {
        PowerMock.replayAll();
        verifyMocks = true;
    }

    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader).anyTimes();
        EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject(Map.class))).andReturn(null).anyTimes();
    }

    @Test
    public void TestOverflowToDisk() throws Exception {
        expectOffsetLookupReturnNone();
        replay();

        task.start(config);

        assertEquals(null, task.poll());

        String query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))";
        server.execute(query);
        String insert_stmt = "INSERT INTO test (data) VALUES('Hello World')";
        server.execute(insert_stmt);

        String update_stmt = "UPDATE test SET data=\'Hello Kitty\' where id=1";
        server.execute(update_stmt);
        server.execute("commit");

        String alter_stmt = "ALTER TABLE test ADD COLUMN extra VARCHAR(3)";
        server.executeQuery(alter_stmt);

        for(int i = 1;;i++) {
            System.out.println("Polling at " + i + " time.");
            if (!pollOnce()) break;
        }

        task.stop();
    }

    @Test
    public void TestEventTypeBlacklist() throws Exception {
        expectOffsetLookupReturnNone();
        replay();

        config.put(MySQLSourceConfig.EVENT_TYPE_BLACKLIST, "FORMAT_DESCRIPTION, XID");
        task.start(config);

        assertEquals(null, task.poll());

        String query = "CREATE TABLE test2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))";
        server.execute(query);
        String insert_stmt = "INSERT INTO test2 (data) VALUES('Hello World')";
        server.execute(insert_stmt);

        String update_stmt = "UPDATE test2 SET data=\'Hello Kitty\' where id=1";
        server.execute(update_stmt);
        server.execute("commit");

        for(int i = 1;;i++) {
            System.out.println("Polling at " + i + " time.");
            if (!pollOnce()) break;
        }

        task.stop();

    }

    @Test
    public void TestTableBlacklist() throws Exception {
        expectOffsetLookupReturnNone();
        replay();

        config.put(MySQLSourceConfig.TABLE_BLACKLIST, "test, test3");
        task.start(config);

        assertEquals(null, task.poll());

        String query = "CREATE TABLE test3 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))";
        server.execute(query);
        String insert_stmt = "INSERT INTO test3 (data) VALUES('Hello World')";
        server.execute(insert_stmt);

        String update_stmt = "INSERT INTO test2 (data) VALUES('Hello Kitty')";
        server.execute(update_stmt);
        server.execute("commit");

        for(int i = 1;;i++) {
            System.out.println("Polling at " + i + " time.");
            if (!pollOnce()) break;
        }

        task.stop();

    }

    private boolean pollOnce() throws InterruptedException {
        List<SourceRecord> evs;
        evs = task.poll();
        if(evs == null) return false;
        for(SourceRecord ev: evs) {
            System.out.println(ev);
        }
        return true;
    }



    @After
    public void tearDown() throws IOException {
        if (verifyMocks) {
            PowerMock.verifyAll();
        }
        client.disconnect();
    }

    @AfterClass
    public static void tearDownDB() throws IOException {

        server.shutDown();
    }
}
