package com.chassis.logminer.cdc.oracle;

import com.chassis.logminer.cdc.connectionfactory.OracleConnectionFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class OracleCDC extends AbstractOracleCDC {

    public void startCDC() throws SQLException {
        OracleConnectionFactory connectionFactory = new OracleConnectionFactory();

        try (Connection connection = connectionFactory.getConnection()) {
            OracleInitializer initializer = new OracleInitializer(connection);

            // Get current redo thread and first SCN
            int redoThread = initializer.getCurrentRedoThread();
            long firstChange = initializer.getFirstScn(redoThread);
            List<OracleLogFile> archivedLogs = initializer.getArchivedLogs(firstChange);

            System.out.println("Starting Oracle CDC from SCN: " + firstChange);
            System.out.println("Redo thread: " + redoThread);
            System.out.println("Archived logs available: " + archivedLogs.size());

            // Add archived logs to LogMiner
            CallableStatement csAddArchivedLogs = connection.prepareCall(OracleStatements.ADD_ARCHIVED_LOG);
            List<String> logFilesNames = archivedLogs.stream()
                    .map(OracleLogFile::getFileName)
                    .collect(Collectors.toList());

            for (int i = 0; i < logFilesNames.size(); i++) {
                csAddArchivedLogs.setInt(1, i == 0 ? 0 : 1);
                csAddArchivedLogs.setString(2, logFilesNames.get(i));
                csAddArchivedLogs.addBatch();
            }

            csAddArchivedLogs.executeBatch();
            csAddArchivedLogs.clearBatch();
            csAddArchivedLogs.close();

            // Start LogMiner session
            CallableStatement csStartLogMiner = connection.prepareCall(OracleStatements.START_LOGMINER);

            OracleScn startScn = archivedLogs.get(0).getFirstScn();
            OracleScn topScnToMine = startScn.add(new OracleScn(new BigInteger(String.valueOf(10000000))));

            csStartLogMiner.setLong(1, startScn.longValue());
            csStartLogMiner.setLong(2, topScnToMine.longValue());
            csStartLogMiner.execute();
            csStartLogMiner.clearParameters();
            csStartLogMiner.close();

            // Start mining changes
            OracleCDCTask cdcTask = new OracleCDCTask();
            cdcTask.startMine(connection);

            // Stop LogMiner
            CallableStatement csStopLogMiner = connection.prepareCall(OracleStatements.STOP_LOGMINER);
            csStopLogMiner.execute();
            csStopLogMiner.close();

            Thread.sleep(10000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Oracle CDC session ended");
        }
    }

    public static void main(String[] args) throws SQLException {
        OracleCDC oracleCDC = new OracleCDC();
        oracleCDC.startCDC();
    }
}
