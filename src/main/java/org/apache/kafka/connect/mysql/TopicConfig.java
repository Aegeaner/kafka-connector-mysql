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


import java.util.List;

public class TopicConfig {
    private final List<String> databasePattern;
    private final List<String> databaseName;
    private final List<String> tablePattern;
    private final List<String> tableName;
    private final String topicPrefix;

    public TopicConfig(List<String> database_pattern, List<String> database_name, List<String> table_pattern, List<String> table_name, String topic_prefix) {
        this.databasePattern = database_pattern;
        this.databaseName = database_name;
        this.tablePattern = table_pattern;
        this.tableName = table_name;
        this.topicPrefix = topic_prefix;
    }

    public List<String> getDatabasePattern() {
        return databasePattern;
    }

    public List<String> getTablePattern() {
        return tablePattern;
    }

    public List<String> getDatabaseName() {
        return databaseName;
    }

    public List<String> getTableName() {
        return tableName;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }
}
