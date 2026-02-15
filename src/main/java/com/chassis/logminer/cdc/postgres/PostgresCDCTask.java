package com.chassis.logminer.cdc.postgres;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PostgresCDCTask {

    private static final String SQL_AND = " AND ";
    private static final int READ_TIMEOUT_MS = 10000;
    private static final int MAX_RECORDS = 10000;

    public void startMine(PGReplicationStream stream, Connection connection) throws SQLException {
        List<String> includeList = new ArrayList<>();
        includeList.add("public.cdc_test");

        String tableFilter = parseTableSchemaList(includeList);
        System.out.println("Monitoring tables: " + tableFilter);

        int count = 0;

        try {
            while (count < MAX_RECORDS) {
                // Read next WAL message with timeout
                ByteBuffer buffer = stream.readPending();

                if (buffer == null) {
                    // No data available, wait a bit
                    TimeUnit.MILLISECONDS.sleep(100);
                    continue;
                }

                // Process the message
                int offset = buffer.arrayOffset();
                byte[] source = buffer.array();
                int length = source.length - offset;
                String message = new String(source, offset, length);

                // Parse the message (simplified - in production use a proper decoder)
                if (StringUtils.isNotEmpty(message)) {
                    System.out.println("CDC Message: " + message);

                    // Determine operation type from message
                    OperationType operation = parseOperationType(message);
                    if (operation != null) {
                        System.out.println("Operation: " + operation);
                        count++;
                    }

                    // Send feedback to server
                    LogSequenceNumber lastReceiveLSN = stream.getLastReceiveLSN();
                    if (lastReceiveLSN != null) {
                        stream.setAppliedLSN(lastReceiveLSN);
                        stream.setFlushedLSN(lastReceiveLSN);
                        stream.forceUpdateStatus();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Total records processed: " + count);
    }

    private OperationType parseOperationType(String message) {
        if (message.contains("\"action\":\"I\"") || message.contains("BEGIN") && message.contains("INSERT")) {
            return OperationType.INSERT;
        } else if (message.contains("\"action\":\"U\"") || message.contains("UPDATE")) {
            return OperationType.UPDATE;
        } else if (message.contains("\"action\":\"D\"") || message.contains("DELETE")) {
            return OperationType.DELETE;
        } else if (message.contains("COMMIT")) {
            return OperationType.COMMIT;
        }
        return null;
    }

    public String parseTableSchemaList(List<String> listSchemaObj) {
        final StringBuilder sb = new StringBuilder(512);
        sb.append("(");

        for (int i = 0; i < listSchemaObj.size(); i++) {
            final String schemaObj = StringUtils.trim(listSchemaObj.get(i));
            boolean escaped = StringUtils.contains(schemaObj, "\"");

            if (schemaObj.contains(".")) {
                final String[] pairSchemaObj = schemaObj.split("\\.");
                final String schemaName = StringUtils.trim(pairSchemaObj[0]);
                final String objName = StringUtils.trim(pairSchemaObj[1]);

                if (StringUtils.equals("%", pairSchemaObj[1]) ||
                        StringUtils.equals("*", pairSchemaObj[1])) {
                    // Only schema name present
                    sb.append("schemaname='");
                    sb.append(escaped ?
                            StringUtils.remove(schemaName, "\"") :
                            schemaName.toLowerCase());
                    sb.append("'");
                } else {
                    // Process pair
                    sb.append("(schemaname='");
                    sb.append(escaped ?
                            StringUtils.remove(schemaName, "\"") :
                            schemaName.toLowerCase());
                    sb.append("'");
                    sb.append(SQL_AND);
                    sb.append("tablename='");
                    sb.append(escaped ?
                            StringUtils.remove(objName, "\"") :
                            objName.toLowerCase());
                    sb.append("')");
                }
            } else {
                // Just plain table name without schema
                sb.append("tablename='");
                sb.append(schemaObj.toLowerCase());
                sb.append("'");
            }

            if (i < listSchemaObj.size() - 1) {
                sb.append(" OR ");
            }
        }

        sb.append(")");
        return sb.toString();
    }

    public enum OperationType {
        INSERT(1),
        UPDATE(2),
        DELETE(3),
        COMMIT(7),
        ROLLBACK(36);

        private final int code;

        OperationType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }
}
