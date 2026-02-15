package com.chassis.logminer.cdc.postgres;

public class PostgresStatements {

    public static final String CURRENT_LSN =
            "SELECT pg_current_wal_lsn()";

    public static final String CHECK_REPLICATION_SLOT =
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = ?";

    public static String createReplicationSlot(String slotName, String plugin) {
        return String.format(
                "SELECT pg_create_logical_replication_slot('%s', '%s')",
                slotName, plugin);
    }

    public static final String DROP_REPLICATION_SLOT =
            "SELECT pg_drop_replication_slot(?)";

    public static final String LIST_WAL_FILES =
            "SELECT name, " +
            "       pg_wal_lsn_diff(lsn, '0/0') as location, " +
            "       size " +
            "FROM pg_ls_waldir() " +
            "ORDER BY name";

    public static final String DATABASE_INFO =
            "SELECT current_database() as name, " +
            "       oid " +
            "FROM pg_database " +
            "WHERE datname = current_database()";

    public static final String LIST_TABLES =
            "SELECT schemaname, tablename, " +
            "       pg_relation_size(schemaname||'.'||tablename) as size " +
            "FROM pg_tables " +
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') " +
            "ORDER BY schemaname, tablename";

    public static final String TABLE_PRIMARY_KEY =
            "SELECT a.attname " +
            "FROM pg_index i " +
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
            "WHERE i.indrelid = ?::regclass " +
            "  AND i.indisprimary";

    public static final String PUBLICATION_EXISTS =
            "SELECT pubname FROM pg_publication WHERE pubname = ?";

    public static String createPublication(String publicationName, String tables) {
        if (tables == null || tables.isEmpty()) {
            return String.format("CREATE PUBLICATION %s FOR ALL TABLES", publicationName);
        } else {
            return String.format("CREATE PUBLICATION %s FOR TABLE %s", publicationName, tables);
        }
    }

    public static final String LIST_PUBLICATIONS =
            "SELECT pubname FROM pg_publication";

    public static final String DECODE_WAL_CHANGES =
            "SELECT lsn, xid, data " +
            "FROM pg_logical_slot_get_changes(?, NULL, NULL, " +
            "     'format-version', '2', " +
            "     'include-xids', 'true', " +
            "     'include-timestamp', 'true')";
}
