package com.chassis.logminer.cdc.core;

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
public class LogMinerIntilizer {

    private final Connection connection;
    private static final String CURRENT = "CURRENT";


    public LogMinerIntilizer(Connection connection){
        this.connection = connection;
    }

    public long getFirstScn(int redoThread) throws SQLException {
        long firstScn = -1;
        try(PreparedStatement ps = this.connection.prepareStatement(LogminerStatments.FIRST_AVAILABLE_SCN_IN_ARCHIVE, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)){
            ps.setInt(1, redoThread);
            try(ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    firstScn = rs.getLong(1);
                }
            }
             return firstScn;
        }
    }


    public int getCurrentRedoThread() throws SQLException {
        int redoThread = 0;
        try (final PreparedStatement psInstance = this.connection.prepareStatement(LogminerStatments.RDBMS_VERSION_AND_MORE,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             final ResultSet rsInstance = psInstance.executeQuery()) {
            if (rsInstance.next()) {
                redoThread = rsInstance.getInt("THREAD#");
            } else {
                throw new SQLException("Unable to read data from V$INSTANCE!");
            }
            return redoThread;
        }
    }

    public List<LogFile> getArchivedLogs(long firstChange) throws SQLException {
        String query = LogminerStatments.allMinableLogsQuery(firstChange, Duration.ZERO, null);

        System.out.println("query :::" + query);

        final List<LogFile> logFiles = new ArrayList<>();
        final Set<LogFile> onlineLogFiles = new LinkedHashSet<>();
        final Set<LogFile> archivedLogFiles = new LinkedHashSet<>();

        try (final Statement psInstance = this.connection.createStatement()) {

            ResultSet rs = psInstance.executeQuery(query);

                while(rs.next()) {
                    String fileName = rs.getString(1);
                    Scn firstScn = getScnFromString(rs.getString(2));
                    Scn nextScn = getScnFromString(rs.getString(3));
                    String status = rs.getString(5);
                    String type = rs.getString(6);
                    BigInteger sequence = new BigInteger(rs.getString(7));
                    int thread = rs.getInt(10);
                    if ("ARCHIVED".equals(type)) {
                        // archive log record
                        LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE, thread);
                       // if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
                         //   LOGGER.trace("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
                            archivedLogFiles.add(logFile);
                       // }
                    }
                    else if ("ONLINE".equals(type)) {
                        LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status), thread);
                    //    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                         //   LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
                            onlineLogFiles.add(logFile);
                       // }
                     //   else {
                     //       LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
                     //   }
                    }
                }

        }

        for (LogFile redoLog : onlineLogFiles) {
            archivedLogFiles.removeIf(f -> {
                if (f.getSequence().equals(redoLog.getSequence())) {
                 //   LOGGER.trace("Removing archive log {} with duplicate sequence {} to {}", f.getFileName(), f.getSequence(), redoLog.getFileName());
                    return true;
                }
                return false;
            });
        }
        logFiles.addAll(archivedLogFiles);
        logFiles.addAll(onlineLogFiles);

        return logFiles;

    }

    private static Scn getScnFromString(String value) {
        if (StringUtils.isEmpty(value)) {
            return Scn.MAX;
        }
        return Scn.valueOf(value);
    }

}
