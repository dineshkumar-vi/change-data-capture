package com.chassis.logminer.cdc.postgres;

import com.chassis.logminer.cdc.connectionfactory.PostgresConnectionFactory;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PostgresCDC extends AbstractPostgresCDC {

    public void startCDC() throws SQLException {
        PostgresConnectionFactory connectionFactory = new PostgresConnectionFactory();

        try (Connection connection = connectionFactory.getConnection()) {
            PostgresInitializer initializer = new PostgresInitializer(connection);

            // Get current LSN and replication slot info
            String slotName = initializer.getOrCreateReplicationSlot();
            LogSequenceNumber startLSN = initializer.getCurrentLSN();
            List<PostgresWalFile> walFiles = initializer.getWalFiles(startLSN);

            System.out.println("Starting PostgreSQL CDC from LSN: " + startLSN);
            System.out.println("Replication slot: " + slotName);
            System.out.println("WAL files available: " + walFiles.size());

            // Create replication connection
            PGConnection replConnection = connection.unwrap(PGConnection.class);

            // Start logical replication stream
            PGReplicationStream stream = replConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slotName)
                    .withStartPosition(startLSN)
                    .withSlotOption("include-xids", true)
                    .withSlotOption("include-timestamp", true)
                    .withSlotOption("pretty-print", false)
                    .start();

            PostgresCDCTask cdcTask = new PostgresCDCTask();
            cdcTask.startMine(stream, connection);

            // Cleanup
            stream.close();

            Thread.sleep(10000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("PostgreSQL CDC session ended");
        }
    }

    public static void main(String[] args) throws SQLException {
        PostgresCDC postgresCDC = new PostgresCDC();
        postgresCDC.startCDC();
    }
}
