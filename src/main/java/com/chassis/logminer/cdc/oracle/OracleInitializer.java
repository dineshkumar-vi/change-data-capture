package com.chassis.logminer.cdc.oracle;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class OracleInitializer {

    private final Connection connection;
    private static final String CURRENT = "CURRENT";

    public OracleInitializer(Connection connection) {
        this.connection = connection;
    }

    public long getFirstScn(int redoThread) throws SQLException {
        long firstScn = -1;
        try (PreparedStatement ps = this.connection.prepareStatement(
                OracleStatements.FIRST_AVAILABLE_SCN_IN_ARCHIVE,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
            ps.setInt(1, redoThread);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    firstScn = rs.getLong(1);
                }
            }
            return firstScn;
        }
    }

    public int getCurrentRedoThread() throws SQLException {
        int redoThread = 0;
        try (final PreparedStatement ps = this.connection.prepareStatement(
                OracleStatements.RDBMS_VERSION_AND_MORE,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
             final ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                redoThread = rs.getInt("THREAD#");
            } else {
                throw new SQLException("Unable to read data from V$INSTANCE!");
            }
            return redoThread;
        }
    }

    public List<OracleLogFile> getArchivedLogs(long firstChange) throws SQLException {
        String query = OracleStatements.allMinableLogsQuery(firstChange, Duration.ZERO, null);

        System.out.println("Query: " + query);

        final List<OracleLogFile> logFiles = new ArrayList<>();
        final Set<OracleLogFile> onlineLogFiles = new LinkedHashSet<>();
        final Set<OracleLogFile> archivedLogFiles = new LinkedHashSet<>();

        try (final Statement stmt = this.connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String fileName = rs.getString(1);
                OracleScn firstScn = getScnFromString(rs.getString(2));
                OracleScn nextScn = getScnFromString(rs.getString(3));
                String status = rs.getString(5);
                String type = rs.getString(6);
                BigInteger sequence = new BigInteger(rs.getString(7));
                int thread = rs.getInt(10);

                if ("ARCHIVED".equals(type)) {
                    OracleLogFile logFile = new OracleLogFile(
                            fileName, firstScn, nextScn, sequence,
                            OracleLogFile.Type.ARCHIVE, thread);
                    archivedLogFiles.add(logFile);
                } else if ("ONLINE".equals(type)) {
                    OracleLogFile logFile = new OracleLogFile(
                            fileName, firstScn, nextScn, sequence,
                            OracleLogFile.Type.REDO,
                            CURRENT.equalsIgnoreCase(status), thread);
                    onlineLogFiles.add(logFile);
                }
            }
        }

        // Remove archived logs with duplicate sequences
        for (OracleLogFile redoLog : onlineLogFiles) {
            archivedLogFiles.removeIf(f -> f.getSequence().equals(redoLog.getSequence()));
        }

        logFiles.addAll(archivedLogFiles);
        logFiles.addAll(onlineLogFiles);

        return logFiles;
    }

    private static OracleScn getScnFromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return OracleScn.MAX;
        }
        return OracleScn.valueOf(value);
    }
}
