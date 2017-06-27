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
import com.github.shyiko.mysql.binlog.event.EventType;
import org.apache.kafka.connect.mysql.MySQLBinlogEventBuffer;
import org.apache.kafka.connect.mysql.MySQLBinlogEventListener;
import org.apache.kafka.connect.mysql.MySQLIsolatedServer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MySQLReplicatorTest {
    private static final long REPLICATOR_TIMEOUT = 5000;
    private static final long EVENT_BUFFER_POLL_TIMEOUT = 1;

    private static MySQLIsolatedServer server;
    private static BinaryLogClient client;
    private static MySQLBinlogEventListener eventListener;
    private static MySQLBinlogEventBuffer eventBuffer;
    private static boolean firstRun = true;
    private static EventRecordFactory eventRecordFactory;

    @BeforeClass
    public static void setUp() throws Exception {
        server = new MySQLIsolatedServer();
        server.boot();

        Integer port = server.getPort();
        client = new BinaryLogClient("localhost", port, "replicator", "replicator");
        eventBuffer = new MySQLBinlogEventBuffer(1024L, 0.5, "events");
        eventListener = new MySQLBinlogEventListener(client, eventBuffer, null, eventRecordFactory);
        client.registerEventListener(eventListener);
        client.connect(REPLICATOR_TIMEOUT);
        ArrayList<String> empty = new ArrayList<>();
        TopicConfig defaultTopicConf = new TopicConfig(empty, empty, empty, empty, "");
        RemoteConfig defaultRemoteConf = new RemoteConfig("localhost", String.valueOf(port), "replicator", "replicator");
        eventRecordFactory = new EventRecordFactory(defaultTopicConf, defaultRemoteConf);
    }

    @Test
    public void testCaptureQueryEvent() throws Exception {
        assertTrue(client.isConnected());

        createTable();
    }

    private static void createTable() throws Exception {
        String query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))";
        server.execute(query);

        if (firstRun) {
            pollOneEventAndVerify(EventType.ROTATE);
            pollOneEventAndVerify(EventType.FORMAT_DESCRIPTION);
            firstRun = false;
        }

        // Query Event for create table
        pollOneEventAndVerify(EventType.QUERY);
    }

    private static void pollOneEventAndVerify(EventType type) throws Exception {
        Thread.sleep(10);
        MySQLBinlogEvent ev = null;
        try {
            ev = eventBuffer.poll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<SourceRecord> record = eventRecordFactory.getSourceRecords(ev, client.getBinlogFilename());
        if (record != null) { System.out.println(record); }
        assertEquals(type, ev.getEventType());
    }

    @After
    public void dropTable() throws Exception {
        String drop_stmt = "DROP TABLE IF EXISTS test";
        server.execute(drop_stmt);
        pollOneEventAndVerify(EventType.QUERY);
    }

    @Test
    public void testCaptureWriteRowEvent() throws Exception {
        createTable();

        String insert_stmt = "INSERT INTO test (data) VALUES('Hello World')";
        server.execute(insert_stmt);
        server.execute("commit");

        // Query Event for the BEGIN
        pollOneEventAndVerify(EventType.QUERY);
        pollOneEventAndVerify(EventType.TABLE_MAP);
        pollOneEventAndVerify(EventType.WRITE_ROWS);
        pollOneEventAndVerify(EventType.XID);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        client.disconnect();
        server.shutDown();
    }
}
