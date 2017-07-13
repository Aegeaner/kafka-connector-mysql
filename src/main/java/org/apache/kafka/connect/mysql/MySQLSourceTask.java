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
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.mysql.MySQLBinlogEventBuffer;
import org.apache.kafka.connect.mysql.MySQLBinlogEventListener;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MySQLSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MySQLSourceTask.class);


    private MySQLBinlogEventBuffer eventBuffer;
    private MySQLBinlogEventListener eventListener;
    private BinaryLogClient client;
    static final Logger logger = LoggerFactory.getLogger(MySQLSourceTask.class);
    private int batch_size;
    private long in_memory_event_size;
    private double memory_ratio;
    private String username;
    private String password;
    private String hostname;
    private int port;
    private List<String> event_type_blacklist;
    private List<String> table_blacklist;
    private String event_cache_file_name;
    private EventRecordFactory eventRecordFactory;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Map<String, Object> configuration = MySQLSourceConfig.CONFIG_DEF.parse(props);

        hostname = (String) configuration.get(MySQLSourceConfig.HOST_CONFIG);
        port = (int) configuration.get(MySQLSourceConfig.PORT_CONFIG);
        batch_size = (int)configuration.get(MySQLSourceConfig.POLL_BATCH_SIZE);
        in_memory_event_size = (long) configuration.get(MySQLSourceConfig.IN_MEMORY_EVENT_SIZE);
        memory_ratio = (double) configuration.get(MySQLSourceConfig.MEMORY_RATIO);
        username = (String) configuration.get(MySQLSourceConfig.USERNAME);
        password = (String) configuration.get(MySQLSourceConfig.PASSWORD);
        event_type_blacklist = (List<String>) configuration.get(MySQLSourceConfig.EVENT_TYPE_BLACKLIST);
        table_blacklist = (List<String>) configuration.get(MySQLSourceConfig.TABLE_BLACKLIST);
        event_cache_file_name = (String) configuration.get(MySQLSourceConfig.EVENT_CACHE_FILE);

        List<String> database_pattern = (List<String>) configuration.get(MySQLSourceConfig.DATABASE_PATTERN);
        List<String> database_name = (List<String>) configuration.get(MySQLSourceConfig.DATABASE_NAME);
        List<String> table_pattern = (List<String>) configuration.get(MySQLSourceConfig.TABLE_PATTERN);
        List<String> table_name = (List<String>) configuration.get(MySQLSourceConfig.TABLE_NAME);
        String topic_prefix = (String) configuration.get(MySQLSourceConfig.TOPIC_PREFIX);
        TopicConfig topicConfig = new TopicConfig(database_pattern, database_name, table_pattern, table_name, topic_prefix);
        RemoteConfig remoteConfig = new RemoteConfig(hostname, String.valueOf(port), username, password);

        this.eventRecordFactory = new EventRecordFactory(topicConfig, remoteConfig);

        setupBinlogClient();
    }

    private void setupBinlogClient() {
        client = new BinaryLogClient(hostname, port, username, password);
        eventBuffer = new MySQLBinlogEventBuffer(in_memory_event_size, memory_ratio, event_cache_file_name);
        eventListener = new MySQLBinlogEventListener(client, hostname, eventBuffer, table_blacklist, eventRecordFactory);
        client.registerEventListener(eventListener);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!client.isConnected()) {
            Map<String, Object> offsetHolder = context.offsetStorageReader().offset(Collections.singletonMap(MySQLSourceConfig.SOURCE_PARTITION_KEY, MySQLSourceConfig.SOURCE_PARTITION_VALUE));
            if (offsetHolder != null) {
                try {
                    String[] offset = ((String) offsetHolder.get(MySQLSourceConfig.OFFSET_KEY)).split(":");
                    String binlogFileName = offset[0];
                    long binlogPosition = Long.valueOf(offset[1]);
                    if (binlogPosition > 0L) {                      // avoid reading binlog from beginning
                        client.setBinlogFilename(binlogFileName);
                        client.setBinlogPosition(binlogPosition);
                        log.debug(binlogFileName + ':' + binlogPosition);
                    }
                } catch (Exception e) {
                    log.warn(e.getMessage());
                }
            }

            try {
                client.connect(MySQLSourceConfig.REPLICATOR_TIMEOUT);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }

        ArrayList<SourceRecord> records = null;
        for(int batch = batch_size; !eventBuffer.empty() && batch >= 0; batch--) {
            if(records == null)
                records = new ArrayList<>();
            MySQLBinlogEvent ev = null;
            try {
                ev = eventBuffer.poll();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            if(ev != null && !isBlacklisted(ev)) {
                ArrayList<SourceRecord> sourceRecords = null;
                try {
                    sourceRecords = eventRecordFactory.getSourceRecords(ev, getBinlogFileName());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                records.addAll(sourceRecords);
            }
        }

        if (records != null && records.isEmpty())
            return null;

        return records;
    }

    private String getBinlogFileName() {
        return client.getBinlogFilename();
    }

    private boolean isBlacklisted(MySQLBinlogEvent ev) {
        String type = ev.getEventType().toString();

        if (!event_type_blacklist.isEmpty() && event_type_blacklist.contains(type)) {
            return true;
        }

        return false;
    }

    @Override
    public void stop() {
        try {
            client.disconnect();
            eventBuffer.stop();
            eventBuffer.clearEventCacheFile();
        } catch (IOException e) {
            log.error(e.getMessage());
        }

    }
}
