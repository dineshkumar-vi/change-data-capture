package com.chassis.logminer.cdc.oracle;

import java.time.Duration;

public class OracleStatements {

    private static final String DATABASE_VIEW = "V$DATABASE";
    private static final String LOG_VIEW = "V$LOG";
    private static final String LOGFILE_VIEW = "V$LOGFILE";
    private static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";
    private static final String ARCHIVE_DEST_STATUS_VIEW = "V$ARCHIVE_DEST_STATUS";

    public static final String START_LOGMINER =
            "begin\n" +
            "  DBMS_LOGMNR.START_LOGMNR(\n" +
            "    STARTSCN => ?,\n" +
            "    ENDSCN => ?,\n" +
            "    OPTIONS => \n" +
            "      DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +\n" +
            "      DBMS_LOGMNR.COMMITTED_DATA_ONLY);\n" +
            "end;\n";

    public static final String STOP_LOGMINER =
            "begin\n" +
            "  DBMS_LOGMNR.END_LOGMNR;\n" +
            "end;\n";

    public static final String ADD_ARCHIVED_LOG =
            "declare\n" +
            "  l_OPTION binary_integer; \n" +
            "begin\n" +
            "  if (? = 0) then\n" +
            "    l_OPTION := DBMS_LOGMNR.NEW;\n" +
            "  else\n" +
            "    l_OPTION := DBMS_LOGMNR.ADDFILE;\n" +
            "  end if;\n" +
            "  DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => l_OPTION);\n" +
            "end;\n";

    public static final String RDBMS_VERSION_AND_MORE =
            "select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME, THREAD#,\n" +
            "(select nvl(CPU_CORE_COUNT_CURRENT, CPU_COUNT_CURRENT) from V$LICENSE) CPU_CORE_COUNT_CURRENT\n" +
            "from V$INSTANCE";

    public static final String FIRST_AVAILABLE_SCN_IN_ARCHIVE =
            "select min(FIRST_CHANGE#)\n" +
            "from V$ARCHIVED_LOG\n" +
            "where ARCHIVED='YES' and STANDBY_DEST='NO' and DELETED='NO'\n" +
            "  and THREAD#=?";

    public static String allMinableLogsQuery(long scn, Duration archiveLogRetention, String archiveDestinationName) {
        final StringBuilder sb = new StringBuilder();

        sb.append("SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, ");
        sb.append("L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END, L.THREAD# AS THREAD ");
        sb.append("FROM ").append(LOGFILE_VIEW).append(" F, ").append(LOG_VIEW).append(" L ");
        sb.append("LEFT JOIN ").append(ARCHIVED_LOG_VIEW).append(" A ");
        sb.append("ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# ");
        sb.append("WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) ");
        sb.append("AND F.GROUP# = L.GROUP# ");
        sb.append("GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD# ");
        sb.append("UNION ");

        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES', ");
        sb.append("NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END, A.THREAD# AS THREAD ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A ");
        sb.append("WHERE A.NAME IS NOT NULL ");
        sb.append("AND A.ARCHIVED = 'YES' ");
        sb.append("AND A.STATUS = 'A' ");
        sb.append("AND A.NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName)).append(") ");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND A.FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24) ");
        }

        return sb.append("ORDER BY 7").toString();
    }

    private static String localArchiveLogDestinationsOnlyQuery(String archiveDestinationName) {
        final StringBuilder query = new StringBuilder(256);
        query.append("SELECT DEST_ID FROM ").append(ARCHIVE_DEST_STATUS_VIEW).append(" WHERE ");
        query.append("STATUS='VALID' AND TYPE='LOCAL' ");
        query.append("AND ROWNUM=1");
        return query.toString();
    }

    public static final String MINE_DATA_CDB =
            "select SCN, TIMESTAMP, OPERATION_CODE, XID, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, DATA_OBJD#, SQL_REDO,\n" +
            "       SRC_CON_UID, (select CON_ID from V$CONTAINERS C where C.CON_UID = L.SRC_CON_UID) CON_ID\n" +
            "from V$LOGMNR_CONTENTS L\n";

    public static final String CHECK_TABLE_CDB =
            "select O.OBJECT_ID, O.CON_ID, T.OWNER, T.TABLE_NAME, T.DEPENDENCIES, P.PDB_NAME,\n" +
            "       decode(O.OBJECT_TYPE, 'TABLE', 'Y', 'N') IS_TABLE,\n" +
            "       decode(O.OBJECT_TYPE, 'TABLE', O.OBJECT_ID,\n" +
            "         (select PT.OBJECT_ID\n" +
            "          from CDB_OBJECTS PT\n" +
            "          where PT.OWNER=O.OWNER\n" +
            "            and PT.OBJECT_NAME=O.OBJECT_NAME\n" +
            "            and PT.CON_ID=O.CON_ID\n" +
            "            and PT.OBJECT_TYPE='TABLE')) PARENT_OBJECT_ID\n" +
            "from CDB_OBJECTS O, CDB_PDBS P, CDB_TABLES T\n" +
            "where O.OBJECT_TYPE in ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')\n" +
            "  and O.TEMPORARY='N'\n" +
            "  and O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER','LBACSYS','DVSYS','WMSYS')\n" +
            "  and O.CON_ID=P.CON_ID (+)\n" +
            "  and O.OWNER=T.OWNER\n" +
            "  and O.OBJECT_NAME=T.TABLE_NAME\n";
}
