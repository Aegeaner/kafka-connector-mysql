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


import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import java.io.Serializable;

public class MySQLBinlogEvent implements Serializable {

    private final EventHeaderV4 header;
    private final long timestamp;
    private final EventType type;
    private final String binlogFilename;
    private final long binlogPosition;
    private final String hostname;
    protected EventData eventdata;


    public MySQLBinlogEvent(Event event, String hostname, String binlogFilename) {
        this.header = event.getHeader();
        this.type = header.getEventType();
        this.hostname = hostname;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = header.getPosition();
        this.timestamp = this.header.getTimestamp();
        this.eventdata = event.getData();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MySQLBinlogEvent{");
        sb.append("eventType=").append(getEventType());
        sb.append(", timestamp=").append(getTimestamp());
        sb.append(", eventData=").append(getEventData());
        sb.append("}");
        return sb.toString();
    }

    public EventType getEventType() {
        return this.type;
    }

    public EventData getEventData() {
        return this.eventdata;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public long getBinlogPosition() {
        return this.binlogPosition;
    }

    public Long getTableId() {
        switch (this.type) {
            case EXT_WRITE_ROWS:
            case WRITE_ROWS:
                return ((WriteRowsEventData) eventdata).getTableId();
            case EXT_UPDATE_ROWS:
            case UPDATE_ROWS:
                return ((UpdateRowsEventData) eventdata).getTableId();
            case EXT_DELETE_ROWS:
            case DELETE_ROWS:
                return ((DeleteRowsEventData) eventdata).getTableId();
            case TABLE_MAP:
                return ((TableMapEventData) eventdata).getTableId();
        }
        return null;
    }

    public String getHostname() {
        return hostname;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }
}
