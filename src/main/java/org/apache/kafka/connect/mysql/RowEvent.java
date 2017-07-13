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


import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import java.io.Serializable;
import java.util.HashMap;

public class RowEvent extends AbstractEvent {

    protected String table;
    private HashMap<String, HashMap<String, Serializable>> row_before;
    private HashMap<String, HashMap<String, Serializable>> row_after;


    public RowEvent(MySQLBinlogEvent event) {
        super(event);
    }

    public void setMetadata(TableMapEventData meta) {
        if (meta == null)
            return;
        this.database = meta.getDatabase();
        this.table = meta.getTable();
    }

    public void setRowAfter(HashMap<String, HashMap<String, Serializable>> row_after) {
        this.row_after = row_after;
    }

    public void setRowBefore(HashMap<String, HashMap<String, Serializable>> row_before) {
        this.row_before = row_before;
    }

    public String getTable() {
        return this.table;
    }
}
