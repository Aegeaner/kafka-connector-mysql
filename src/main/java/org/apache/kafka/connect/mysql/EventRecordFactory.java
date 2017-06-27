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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventRecordFactory {

    private static final Logger log = LoggerFactory.getLogger(EventRecordFactory.class);
    private final ConcurrentHashMap<Long, TableMapEventData> tableMetaCache;
    private final ConcurrentHashMap<String, List<Column>> columnMetaCache;
    private final TopicConfig topicConfig;
    private final RemoteConfig remoteConfig;
    private final List<String> databaseName;
    private final List<String> tableName;
    private List<Pattern> databasePattern = new ArrayList<>();
    private List<Pattern> tablePattern = new ArrayList<>();

    public EventRecordFactory(TopicConfig topicConfig, RemoteConfig remoteConfig) {
        this.topicConfig = topicConfig;
        this.remoteConfig = remoteConfig;
        this.databaseName = topicConfig.getDatabaseName();
        this.tableName = topicConfig.getTableName();

        this.tableMetaCache = new ConcurrentHashMap<>();
        this.columnMetaCache = new ConcurrentHashMap<>();

        if (!topicConfig.getDatabasePattern().isEmpty() && !topicConfig.getDatabaseName().isEmpty()) {
            for (String dbPatternStr: topicConfig.getDatabasePattern()) {
                this.databasePattern.add(Pattern.compile(dbPatternStr));
            }
        }
        if (!topicConfig.getTablePattern().isEmpty() && !topicConfig.getTableName().isEmpty()) {
            for (String tbPatternStr: topicConfig.getTablePattern()) {
                this.tablePattern.add(Pattern.compile(tbPatternStr));
            }
        }
    }

    public ArrayList<SourceRecord> getSourceRecords(MySQLBinlogEvent rawEvent, String binlogFileName) throws Exception {

        EventType type = rawEvent.getEventType();
        TableMapEventData meta;
        AbstractEvent event;
        ArrayList<SourceRecord> recordList = new ArrayList<>();

        switch (type) {
            case TABLE_MAP:
                TableMapEventData eventData = (TableMapEventData) rawEvent.getEventData();
                long tableId = eventData.getTableId();
                tableMetaCache.put(tableId, eventData);
                event = new TableMapEvent(rawEvent);
                String newTable = eventData.getTable();
                if (!this.tablePattern.isEmpty()) {
                    newTable = getAggregatedName(eventData.getTable(), this.tablePattern, this.tableName);
                }

                List<Column> columns;
                if (!columnMetaCache.containsKey(newTable)) {
                    columns = fetchColumns(remoteConfig, eventData.getDatabase(), eventData.getTable());
                    columnMetaCache.put(newTable, columns);
                }
                break;
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                WriteRowsEventData writeData = (WriteRowsEventData) rawEvent.getEventData();
                Long writeTableId = rawEvent.getTableId();
                meta = tableMetaCache.get(writeTableId);

                for ( Serializable[] data : writeData.getRows() ) {
                    event = new RowEvent(rawEvent);
                    ((RowEvent)event).setMetadata(meta);
                    String aggregatedTableName = buildTableName(event);
                    String database =  event.getDatabase();
                    String table = ((RowEvent) event).getTable();
                    HashMap<String, HashMap<String, Serializable>> schematized = schematizeRow(data, database, table, aggregatedTableName);
                    ((RowEvent)event).setRowAfter(schematized);
                    buildOneRecord(type, event, binlogFileName, recordList);
                }
                return recordList;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsEventData updateData = (UpdateRowsEventData) rawEvent.getEventData();
                Long updateTableId = rawEvent.getTableId();
                meta = tableMetaCache.get(updateTableId);

                for (Map.Entry<Serializable[], Serializable[]> data : updateData.getRows()) {
                    event = new RowEvent(rawEvent);
                    ((RowEvent)event).setMetadata(meta);
                    String aggregatedTableName = buildTableName(event);
                    String database =  event.getDatabase();
                    String table = ((RowEvent) event).getTable();
                    HashMap<String, HashMap<String, Serializable>> schematized1 = schematizeRow(data.getKey(), database, table, aggregatedTableName);
                    HashMap<String, HashMap<String, Serializable>> schematized2 = schematizeRow(data.getValue(), database, table, aggregatedTableName);
                    ((RowEvent)event).setRowBefore(schematized1);
                    ((RowEvent)event).setRowAfter(schematized2);
                    buildOneRecord(type, event, binlogFileName, recordList);
                }
                return recordList;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsEventData deleteData = (DeleteRowsEventData) rawEvent.getEventData();
                Long deleteTableId = rawEvent.getTableId();
                meta = tableMetaCache.get(deleteTableId);

                for(Serializable[] data: deleteData.getRows()) {
                    event = new RowEvent(rawEvent);
                    ((RowEvent)event).setMetadata(meta);
                    String aggregatedTableName = buildTableName(event);
                    String database =  event.getDatabase();
                    String table = ((RowEvent) event).getTable();
                    HashMap<String, HashMap<String, Serializable>> schematized = schematizeRow(data, database, table, aggregatedTableName);
                    ((RowEvent)event).setRowBefore(schematized);
                    buildOneRecord(type, event, binlogFileName, recordList);
                }
                return recordList;
            case QUERY:
                QueryEventData ddlEventData = (QueryEventData)rawEvent.getEventData();
                String ddlSqlStr = ddlEventData.getSql();
                if (!ddlSqlStr.equals("BEGIN")) {
                    event = new QueryEvent(rawEvent);
                    if (ddlSqlStr.toUpperCase().startsWith("ALTER TABLE")) {
                        String table = ddlSqlStr.split("\\s+", 4)[2];
                        String aggTableName;
                        if (!this.tablePattern.isEmpty()) {
                            aggTableName = getAggregatedName(table, this.tablePattern, this.tableName);
                        } else {
                            aggTableName = table;
                        }
                        columns = fetchColumns(remoteConfig, ddlEventData.getDatabase(), table);
                        columnMetaCache.put(aggTableName, columns);
                    }
                } else {
                    return recordList;
                }
                break;
            case ROTATE:
            case FORMAT_DESCRIPTION:
                return recordList;                             // skip ROTATE and FORMAT_DESCRIPTION event to avoid negative binlog position
            default:
                if (type == EventType.ROTATE) {
                    tableMetaCache.clear();
                }
                event = new AbstractEvent(rawEvent);
                break;
        }

        buildOneRecord(type, event, binlogFileName, recordList);
        return recordList;
    }


    private HashMap<String, HashMap<String, Serializable>> schematizeRow(Serializable[] data, String database, String table, String aggregatedTableName) {
        HashMap<String, HashMap<String, Serializable>> row = new HashMap<>();
        List<Column> columns;

        if(!this.tablePattern.isEmpty() && !columnMetaCache.containsKey(aggregatedTableName)) {
            columns = fetchColumns(remoteConfig, database, table);
            columnMetaCache.put(aggregatedTableName, columns);
        } else if(this.tablePattern.isEmpty()) {
            columns = columnMetaCache.get(table);
        } else {
            columns = columnMetaCache.get(aggregatedTableName);
        }

        for(int i=0; i<data.length; i++) {
            Column col = columns.get(i);
            HashMap<String, Serializable> record = new HashMap<>();
            record.put("val", data[i]);
            record.put("type", col.getDataType());
            row.put(col.getColumnName(), record);
        }
        return row;
    }

    private List<Column> fetchColumns(RemoteConfig remoteConfig, String tableSchema, String tableName) {
        String host = remoteConfig.getHost();
        String port = remoteConfig.getPort();
        String usr = remoteConfig.getUser();
        String passwd = remoteConfig.getPassword();
        return fetchColumns(host, port, usr, passwd, tableSchema, tableName);
    }

    private List<Column> fetchColumns(String host, String port, String usr, String passwd, String tableSchema, String tableName) {
        Statement stmt = null;
        ResultSet rs = null;
        Connection conn;
        ArrayList<Column> columnList = new ArrayList<>();

        try {

            conn = getSQLConnection(host, port, usr, passwd);
            stmt = conn.createStatement();
            final StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS");
            sqlBuilder.append(" where TABLE_SCHEMA=\'").append(tableSchema).append("\'");
            sqlBuilder.append(" and TABLE_NAME=\'").append(tableName).append("\'");
            rs = stmt.executeQuery(sqlBuilder.toString());
            log.warn(sqlBuilder.toString());

            while(rs.next()) {
                String columnName =  rs.getString("COLUMN_NAME");
                String dataType = rs.getString("DATA_TYPE");
                Column column = new Column(columnName, dataType);
                columnList.add(column);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore
                stmt = null;
            }
        }

        return columnList;
    }

    private Connection getSQLConnection(String host, String port, String user, String passwd) throws SQLException {
        String url = "jdbc:mysql://" + host + ":" + port;
        return DriverManager.getConnection(url, user, passwd);
    }

    private void buildOneRecord(EventType type, AbstractEvent event, String binlogFileName, ArrayList<SourceRecord> recordList) {
        SourceRecord newRecord;
        try {
            newRecord = buildSourceRecord(type, event, binlogFileName);
            recordList.add(newRecord);
        } catch(Exception e) {
            // filtered event
        }
    }

    private SourceRecord buildSourceRecord(EventType type, AbstractEvent event, String binlogFileName) throws Exception {
        Map<String, String> partition = Collections.singletonMap(MySQLSourceConfig.SOURCE_PARTITION_KEY, MySQLSourceConfig.SOURCE_PARTITION_VALUE);
        String binlogOffset = binlogFileName + ':' + event.getOffset();

        Map<String, ?> offset = Collections.singletonMap(MySQLSourceConfig.OFFSET_KEY, binlogOffset);

        StringBuilder sb = new StringBuilder();

        String database = buildDatabaseName(event);
        if (!topicConfig.getTopicPrefix().isEmpty()) {
            sb.append(topicConfig.getTopicPrefix());
            sb.append('-');
        }
        sb.append(database);

        if (event.isRowType()) {
            sb.append('-');
            String table = buildTableName(event);
            sb.append(table);
        }

        String topic = sb.toString();

        long timestamp = event.getTimestamp();
        if (timestamp == 0L) {
            Timestamp ts = new Timestamp(System.currentTimeMillis()); // avoid invalid timestamp exception
            timestamp = ts.getTime();
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        String value = null;
        try {
            value = mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }

        return new SourceRecord(partition, offset, topic, null, null, null, null, value, timestamp);
    }

    private String buildTableName(AbstractEvent event) {
        String originTableName = ((RowEvent)event).getTable();

        if (topicConfig.getTablePattern().isEmpty() || this.tableName.isEmpty()) {
            return originTableName;
        } else {
            return getAggregatedName(originTableName, this.tablePattern, this.tableName);
        }
    }

    private String buildDatabaseName(AbstractEvent event)  {
        String originDatabaseName = event.getDatabase();

        if (topicConfig.getDatabasePattern().isEmpty() || this.databaseName.isEmpty()) {
            return originDatabaseName;
        } else {
            return getAggregatedName(originDatabaseName, this.databasePattern, this.databaseName);
        }
    }

    private String getAggregatedName(String originName, List<Pattern> patternList, List<String> patternName) {
        int i = 0;
        for (Pattern tbPattern: patternList) {
            Matcher matcher = tbPattern.matcher(originName);
            boolean matches = matcher.lookingAt();
            if (matches) {
                return patternName.get(i);
            }
            i++;
        }
        log.debug("Pattern mismatch: {}", originName);
        return originName;
    }

    public boolean checkTableBlacklist(MySQLBinlogEvent ev, List<String> table_blacklist) {
        Long tableId = ev.getTableId();
        if (tableId != null) {
            TableMapEventData meta = tableMetaCache.get(tableId);
            if (meta != null) {
                String tableName = meta.getTable();
                if (table_blacklist.contains(tableName)) {
                    log.debug("BLACKLISTED event in table: {}", tableName);
                    return true;
                }
            } else {
                log.debug("Table map cache is null");
            }
        }

        return false;
    }
}
