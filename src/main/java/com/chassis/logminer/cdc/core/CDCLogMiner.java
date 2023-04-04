package com.chassis.logminer.cdc.core;

import com.chassis.logminer.cdc.connectionfactory.CDCConnectionFactory;
import oracle.jdbc.driver.OracleConnection;

import java.math.BigInteger;
import java.sql.*;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CDCLogMiner extends AbstractLogMiner{



    public void startCDC() throws SQLException {


        CDCConnectionFactory cdcConnectionFactory = new CDCConnectionFactory();

        try(Connection connection = cdcConnectionFactory.getConnection()) {

            LogMinerIntilizer logMinerIntilizer =  new LogMinerIntilizer(connection);

            int redoThread   = logMinerIntilizer.getCurrentRedoThread();
            long firstChange = logMinerIntilizer.getFirstScn(redoThread);
            List<LogFile> archivedLogs = logMinerIntilizer.getArchivedLogs(firstChange);



            CallableStatement csAddArchivedLogs = connection.prepareCall(LogminerStatments.ADD_ARCHIVED_LOG);
            List<String> logFilesNames = archivedLogs.stream().map(LogFile::getFileName).collect(Collectors.toList());


                for(String file : logFilesNames) {
                    csAddArchivedLogs.setInt(1, 1);
                    csAddArchivedLogs.setString(2, file);
                    csAddArchivedLogs.addBatch();
                }




           csAddArchivedLogs.executeBatch();
           csAddArchivedLogs.clearBatch();



            CallableStatement callableStatement2 = connection.prepareCall(LogminerStatments.START_LOGMINER);


            Scn startScn = archivedLogs.get(0).getFirstScn();

            Scn topScnToMine = startScn.add(new Scn(new BigInteger(String.valueOf(10000000))));

            callableStatement2.setLong(1, startScn.longValue());


            callableStatement2.setLong(2, topScnToMine.longValue());
            callableStatement2.execute();
            callableStatement2.clearParameters();

            CdcTask cdcTask = new CdcTask();
            cdcTask.startMine(connection);

            CallableStatement callableStatement = connection.prepareCall(LogminerStatments.STOP_LOGMINER);
            callableStatement.execute();

            Thread.sleep(10000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }
    }

    private static Scn getMinimumScn(List<LogFile> logs) {
        return logs.stream().map(LogFile::getFirstScn).min(Scn::compareTo).orElse(Scn.NULL);
    }


    public static void main(String[] args) throws SQLException {
        CDCLogMiner cdcLogMiner = new CDCLogMiner();
        cdcLogMiner.startCDC();
    }

    private Scn getMaxArchiveLogScn(List<LogFile> logFiles) {
        if (logFiles == null || logFiles.isEmpty()) {
            //throw new DebeziumException("Cannot get maximum archive log SCN as no logs were available.");
        }

        final List<LogFile> archiveLogs = logFiles.stream()
                .filter(log -> log.getType().equals(LogFile.Type.ARCHIVE))
                .collect(Collectors.toList());

        if (archiveLogs.isEmpty()) {
           // throw new DebeziumException("Cannot get maximum archive log SCN as no archive logs are present.");
        }

        Scn maxScn = archiveLogs.get(0).getNextScn();
        for (int i = 1; i < archiveLogs.size(); ++i) {
            Scn nextScn = archiveLogs.get(i).getNextScn();
            if (nextScn.compareTo(maxScn) > 0) {
                maxScn = nextScn;
            }
        }

        //LOGGER.debug("Maximum archive log SCN resolved as {}", maxScn);
        return maxScn;
    }


}
