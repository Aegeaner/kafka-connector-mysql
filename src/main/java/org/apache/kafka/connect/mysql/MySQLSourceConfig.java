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


import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;

public class MySQLSourceConfig {
    public static final String OFFSET_KEY = "Offset";
    public static String SOURCE_PARTITION_KEY = "Partition";
    public static String SOURCE_PARTITION_VALUE = "MySQL";
    public static final long REPLICATOR_TIMEOUT = 5000;

    public static final String HOST_CONFIG = "host";
    public static final String PORT_CONFIG = "port";
    public static final String POLL_BATCH_SIZE = "poll_batch_size";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String IN_MEMORY_EVENT_SIZE = "in_memory_event_size";
    public static final String MEMORY_RATIO = "memory_ratio";
    public static final String EVENT_TYPE_BLACKLIST = "event_type_blacklist";
    public static final String TABLE_BLACKLIST = "table_blacklist";
    public static final String EVENT_CACHE_FILE = "event_cache_file";
    public static final String DATABASE_PATTERN = "database_pattern";
    public static final String TABLE_PATTERN = "table_pattern";
    public static final String TOPIC_PREFIX = "topic_prefix";
    public static final String DATABASE_NAME = "database_name";
    public static final String TABLE_NAME = "table_name";
    private static final ArrayList<String> empty = new ArrayList<>();

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HOST_CONFIG, ConfigDef.Type.STRING, "localhost", ConfigDef.Importance.HIGH, "DB Master host")
            .define(PORT_CONFIG, ConfigDef.Type.INT, 3306, ConfigDef.Importance.HIGH, "DB Master port")
            .define(POLL_BATCH_SIZE, ConfigDef.Type.INT, 100, ConfigDef.Importance.HIGH, "Task poll batch size")
            .define(USERNAME, ConfigDef.Type.STRING, "replicator", ConfigDef.Importance.HIGH, "DB Username")
            .define(PASSWORD, ConfigDef.Type.STRING, "replicator", ConfigDef.Importance.HIGH, "DB Password")
            .define(IN_MEMORY_EVENT_SIZE, ConfigDef.Type.LONG, 1024L, ConfigDef.Importance.HIGH, "In memory event size")
            .define(MEMORY_RATIO, ConfigDef.Type.DOUBLE, 0.5, ConfigDef.Importance.HIGH, "Memory ratio limit")
            .define(EVENT_TYPE_BLACKLIST, ConfigDef.Type.LIST, "ROTATE, FORMAT_DESCRIPTION, XID", ConfigDef.Importance.HIGH, "Event type blacklist")
            .define(TABLE_BLACKLIST, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, "Table blacklist")
            .define(EVENT_CACHE_FILE, ConfigDef.Type.STRING, "events", ConfigDef.Importance.HIGH, "Event cache file name")
            // aggregate database name & table name
            .define(DATABASE_PATTERN, ConfigDef.Type.LIST, empty, ConfigDef.Importance.MEDIUM, "Database name pattern")
            .define(DATABASE_NAME, ConfigDef.Type.LIST, empty, ConfigDef.Importance.MEDIUM, "Database name aggregated")
            .define(TABLE_PATTERN, ConfigDef.Type.LIST, empty, ConfigDef.Importance.MEDIUM, "Table name pattern")
            .define(TABLE_NAME, ConfigDef.Type.LIST, empty, ConfigDef.Importance.MEDIUM, "Table name aggregated")
            .define(TOPIC_PREFIX, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Topic name prefix");

}
