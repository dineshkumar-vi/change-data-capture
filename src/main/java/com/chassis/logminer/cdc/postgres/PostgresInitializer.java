package com.chassis.logminer.cdc.postgres;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class PostgresInitializer {

    private final Connection connection;
    private static final String DEFAULT_SLOT_NAME = "cdc_slot";
    private static final String OUTPUT_PLUGIN = "pgoutput";

    public PostgresInitializer(Connection connection) {
        this.connection = connection;
    }

    public LogSequenceNumber getCurrentLSN() throws SQLException {
        String query = PostgresStatements.CURRENT_LSN;
        try (PreparedStatement ps = this.connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String lsn = rs.getString(1);
                return LogSequenceNumber.valueOf(lsn);
            }
            throw new SQLException("Unable to read current LSN from database");
        }
    }

    public String getOrCreateReplicationSlot() throws SQLException {
        // Check if replication slot exists
        String checkQuery = PostgresStatements.CHECK_REPLICATION_SLOT;
        try (PreparedStatement ps = this.connection.prepareStatement(checkQuery)) {
            ps.setString(1, DEFAULT_SLOT_NAME);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.println("Replication slot already exists: " + DEFAULT_SLOT_NAME);
                    return DEFAULT_SLOT_NAME;
                }
            }
        }

        // Create replication slot if it doesn't exist
        String createQuery = PostgresStatements.createReplicationSlot(DEFAULT_SLOT_NAME, OUTPUT_PLUGIN);
        try (Statement stmt = this.connection.createStatement()) {
            stmt.execute(createQuery);
            System.out.println("Created replication slot: " + DEFAULT_SLOT_NAME);
        }

        return DEFAULT_SLOT_NAME;
    }

    public List<PostgresWalFile> getWalFiles(LogSequenceNumber fromLSN) throws SQLException {
        String query = PostgresStatements.LIST_WAL_FILES;
        System.out.println("WAL query: " + query);

        final List<PostgresWalFile> walFiles = new ArrayList<>();

        try (Statement stmt = this.connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String fileName = rs.getString("name");
                String location = rs.getString("location");
                long size = rs.getLong("size");

                PostgresWalFile walFile = new PostgresWalFile(fileName, location, size);
                walFiles.add(walFile);
            }
        }

        System.out.println("Found " + walFiles.size() + " WAL files");
        return walFiles;
    }

    public int getCurrentDatabaseOid() throws SQLException {
        try (final PreparedStatement ps = this.connection.prepareStatement(
                PostgresStatements.DATABASE_INFO,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             final ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("oid");
            } else {
                throw new SQLException("Unable to read database info!");
            }
        }
    }
}
