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


import com.github.shyiko.mysql.binlog.event.EventType;
import org.apache.kafka.connect.mysql.MySQLBinlogEvent;

public class AbstractEvent {
    protected final EventType type;
    protected final long timestamp;
    protected final long binlog_position;
    protected final String db_hostname;
    protected final String binlog_filename;
    protected String database;

    public AbstractEvent(MySQLBinlogEvent event) {
        this.timestamp = event.getTimestamp();
        this.type = event.getEventType();
        this.db_hostname = event.getHostname();
        this.binlog_filename = event.getBinlogFilename();
        this.binlog_position = event.getBinlogPosition();
        this.database = null;
    }

    public long getBinlogPosition() {
        return binlog_position;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getDatabase() {
        return database;
    }

    public boolean isRowType() {
        return (type == EventType.WRITE_ROWS || type == EventType.EXT_WRITE_ROWS
                || type == EventType.DELETE_ROWS || type == EventType.EXT_DELETE_ROWS
                || type == EventType.UPDATE_ROWS || type == EventType.EXT_UPDATE_ROWS);
    }
}
