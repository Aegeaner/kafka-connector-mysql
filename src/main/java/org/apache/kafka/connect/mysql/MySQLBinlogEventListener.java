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
import com.github.shyiko.mysql.binlog.event.Event;
import org.apache.kafka.connect.mysql.EventRecordFactory;
import org.apache.kafka.connect.mysql.MySQLBinlogEventBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class MySQLBinlogEventListener implements BinaryLogClient.EventListener {
    private static final Logger log = LoggerFactory.getLogger(MySQLBinlogEventListener.class);
    private final MySQLBinlogEventBuffer eventBuffer;
    private final BinaryLogClient client;
    private final List<String> tableBlacklist;
    private final String hostname;
    private EventRecordFactory eventRecordFactory;

    public MySQLBinlogEventListener(BinaryLogClient client, String hostname, MySQLBinlogEventBuffer eventBuffer, List<String> table_blacklist, EventRecordFactory eventRecordFactory) {
        this.client = client;
        this.hostname = hostname;
        this.eventBuffer = eventBuffer;
        this.tableBlacklist = table_blacklist;
        this.eventRecordFactory = eventRecordFactory;
    }

    @Override
    public void onEvent(Event event) {
        MySQLBinlogEvent ev = new MySQLBinlogEvent(event, hostname, client.getBinlogFilename());
        try {
            if (tableBlacklist == null || !eventRecordFactory.checkTableBlacklist(ev, tableBlacklist)) {
                eventBuffer.put(ev);
            } else {
                log.debug("Aborted: {}", ev);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
