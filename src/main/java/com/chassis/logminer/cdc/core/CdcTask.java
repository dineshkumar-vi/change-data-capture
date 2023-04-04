package com.chassis.logminer.cdc.core;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CdcTask {

    public static final int MODE_WHERE_ALL_MVIEW_LOGS = 1;
    public static final int MODE_WHERE_ALL_OBJECTS = 2;
    private static final String SQL_AND = " and ";
    private OraclePreparedStatement psLogMiner;

    private boolean cdb = true;

    private boolean pdbConnectionAllowed = false;


    public static final String MINE_DATA_CDB =
            "select SCN, TIMESTAMP, OPERATION_CODE, XID, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, DATA_OBJD#, SQL_REDO,\n" +
                    "       SRC_CON_UID, (select CON_ID from V$CONTAINERS C where C.CON_UID = L.SRC_CON_UID) CON_ID\n" +
                    "from   V$LOGMNR_CONTENTS L\n";


    public static final String CHECK_TABLE_CDB =
            "select O.OBJECT_ID, O.CON_ID, T.OWNER, T.TABLE_NAME, T.DEPENDENCIES, P.PDB_NAME,\n" +
                    "       decode(O.OBJECT_TYPE, 'TABLE', 'Y', 'N') IS_TABLE,\n" +
                    "       decode(O.OBJECT_TYPE, 'TABLE', O.OBJECT_ID,\n" +
                    "         (select PT.OBJECT_ID\n" +
                    "          from   CDB_OBJECTS PT\n" +
                    "          where  PT.OWNER=O.OWNER\n" +
                    "            and  PT.OBJECT_NAME=O.OBJECT_NAME\n" +
                    "            and  PT.CON_ID=O.CON_ID\n" +
                    "            and  PT.OBJECT_TYPE='TABLE')) PARENT_OBJECT_ID\n" +
                    "from   CDB_OBJECTS O, CDB_PDBS P, CDB_TABLES T\n" +
                    "where  O.OBJECT_TYPE in ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')\n" +
                    "  and  O.TEMPORARY='N'\n" +
                    "  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER','LBACSYS','DVSYS','WMSYS')\n" +
                    "  and  O.CON_ID=P.CON_ID (+)\n" +
                    "  and  O.OWNER=T.OWNER\n" +
                    "  and  O.OBJECT_NAME=T.TABLE_NAME\n";
    public static final String CHECK_TABLE_CDB_WHERE_PARAM =
            "  and  O.OBJECT_ID=?\n" +
                    "  and  O.CON_ID=?\n";

    public static final String INITIAL_LOAD_LIST_CDB =
            "select O.OBJECT_ID, O.CON_ID, T.OWNER, T.TABLE_NAME, T.DEPENDENCIES, P.PDB_NAME\n" +
                    "from   CDB_OBJECTS O, CDB_PDBS P, CDB_TABLES T\n" +
                    "where  O.OBJECT_TYPE='TABLE'\n" +
                    "  and  O.TEMPORARY='N'\n" +
                    "  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER','LBACSYS','DVSYS','WMSYS')\n" +
                    "  and  O.CON_ID=P.CON_ID (+)\n" +
                    "  and  O.OWNER=T.OWNER\n" +
                    "  and  O.OBJECT_NAME=T.TABLE_NAME";


    public void startMine(Connection connection) throws SQLException {

        String mineSQL = MINE_DATA_CDB;
        String checkTableSql = CHECK_TABLE_CDB + CHECK_TABLE_CDB_WHERE_PARAM;

        String initialLoadSql = INITIAL_LOAD_LIST_CDB;

        List<String> includeList = new ArrayList<>();
        includeList.add("USER_DETAILS");


        final String tableList = parseTableSchemaList(
                false, MODE_WHERE_ALL_OBJECTS, includeList);


        final String objectList = getMineObjectsIds(
                connection, false, tableList);

        mineSQL += "where ((OPERATION_CODE in (1,2,3,5) " +  objectList + ")";

        checkTableSql +=tableList;

        // mineSQL += "where OPERATION_CODE in (1) ";

        mineSQL += " or OPERATION_CODE in (7,36))";

        mineSQL += getConUidsList(connection);


        mineSQL = "SELECT SCN, USERNAME, OPERATION_CODE, XID, TIMESTAMP, SQL_REDO, TABLE_NAME, COMMIT_SCN , SEQUENCE#, CSF, XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN, SEG_OWNER, ROLLBACK, ROW_ID FROM V$LOGMNR_CONTENTS  WHERE OPERATION_CODE in (1, 2, 3, 5) AND  SEG_OWNER in ('C##CRUDUSER') AND SEG_NAME ='CDC_TEST'";

        int count = 0;

        try (OraclePreparedStatement psLogMiner =   (OraclePreparedStatement) connection.prepareStatement(
                mineSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            //psLogMiner.setRowPrefetch(100);



            OracleResultSet oracleResultSet = (OracleResultSet) psLogMiner.executeQuery();

            while (oracleResultSet.next()) {
                short operation = oracleResultSet.getShort("OPERATION_CODE");

                String xid = oracleResultSet.getString("XID");
                Long lastScn = oracleResultSet.getLong("SCN");
                String lastRsId = oracleResultSet.getString("RS_ID");
                Long lastSsn = oracleResultSet.getLong("SSN");

                String redo = oracleResultSet.getString("SQL_REDO");
                System.out.println("operation ==> " + operation);
                if(! StringUtils.equalsAnyIgnoreCase(redo, "commit") && ! StringUtils.equalsAnyIgnoreCase(redo, "rollback")) {
                    System.out.println( redo );
                }
            }
        }

        System.out.println("Total count "+ count);
    }


    private String readSqlRedo( ResultSet oracleResultSet) throws SQLException {
        final boolean multiLineSql = oracleResultSet.getBoolean("CSF");
        if (multiLineSql) {
            final StringBuilder sb = new StringBuilder(16000);
            boolean moreRedoLines = multiLineSql;
            while (moreRedoLines) {
                sb.append(oracleResultSet.getString("SQL_REDO"));
                moreRedoLines = oracleResultSet.getBoolean("CSF");
                if (moreRedoLines) {
                    oracleResultSet.next();
                }
            }
            return sb.toString();
        } else {
            return oracleResultSet.getString("SQL_REDO");
        }
    }


    public String getMineObjectsIds(final Connection connection,
                                    final boolean exclude, final String where) throws SQLException {
        final StringBuilder sb = new StringBuilder(32768);
        if (exclude) {
            sb.append(" and DATA_OBJ# not in (");
        } else {
            sb.append(" and (DATA_OBJ# in (");
        }

        //TODO
        //TODO For CDB - pair required!!!
        //TODO OBJECT_ID is not unique!!!
        //TODO Need to add "a2.static.objects" parameter for using this for predicate
        //TODO
        final String selectObjectIds =
                "select OBJECT_ID\n" +
                        ((cdb && !pdbConnectionAllowed) ? "from   CDB_OBJECTS O\n" : "from   DBA_OBJECTS O\n") +
                        "where  DATA_OBJECT_ID is not null\n" +
                        "  and  OBJECT_TYPE like 'TABLE%'\n" +
                        "  and  TEMPORARY='N'\n" +
                        //((cdb && !pdbConnectionAllowed) ? "  and  CON_ID > 2\n" : "") +
                        where;

        System.out.println(selectObjectIds);

        PreparedStatement ps = connection.prepareStatement(selectObjectIds,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = ps.executeQuery();
        boolean firstValue = true;
        boolean lastValue = false;
        int recordCount = 0;
        while (rs.next()) {
            lastValue = false;
            if (firstValue) {
                firstValue = false;
            } else {
                sb.append(",");
            }
            sb.append(rs.getInt(1));
            recordCount++;
            if (recordCount > 999) {
                // ORA-01795
                sb.append(")");
                lastValue = true;
                if (exclude) {
                    sb.append(" and DATA_OBJ# not in (");
                } else {
                    sb.append(" or DATA_OBJ# in (");
                }
                firstValue = true;
                recordCount = 0;
            }
        }
        if (!lastValue) {
            sb.append(")");
        }
        sb.append(")");
        rs.close();
        rs = null;
        ps.close();
        ps = null;
        System.out.println("===== Object List"+ sb.toString());
        return sb.toString();
    }

    public static String parseTableSchemaList(final boolean exclude, final int mode, final List<String> listSchemaObj) {
        final String schemaNameField;
        final String objNameField;
        if (mode == MODE_WHERE_ALL_MVIEW_LOGS) {
            schemaNameField = "L.LOG_OWNER";
            objNameField = "L.MASTER";
        } else {
            schemaNameField = "O.OWNER";
            objNameField = "O.OBJECT_NAME";
        }

        final StringBuilder sb = new StringBuilder(512);
        sb.append(SQL_AND);
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
                    sb.append("(");
                    sb.append(schemaNameField);
                    sb.append(exclude ? "!='" : "='");
                    sb.append(escaped ?
                            StringUtils.remove(schemaName, "\"") :
                            StringUtils.upperCase(schemaName));
                    sb.append("')");
                } else {
                    // Process pair... ... ...
                    sb.append("(");
                    sb.append(schemaNameField);
                    sb.append("='");
                    sb.append(escaped ?
                            StringUtils.remove(schemaName, "\"") :
                            StringUtils.upperCase(schemaName));
                    sb.append("'");
                    sb.append(SQL_AND);
                    sb.append(objNameField);
                    sb.append(exclude ? "!='" : "='");
                    sb.append(escaped ?
                            StringUtils.remove(objName, "\"") :
                            StringUtils.upperCase(objName));
                    sb.append("')");
                }
            } else {
                // Just plain table name without owner
                sb.append("(");
                sb.append(objNameField);
                sb.append(exclude ? "!='" : "='");
                sb.append(schemaObj);
                sb.append("')");
            }
            if (i < listSchemaObj.size() - 1) {
                if (exclude)
                    sb.append(SQL_AND);
                else
                    sb.append(" or ");
            }
        }

        sb.append(")");
        return sb.toString();
    }

    public String getConUidsList(final Connection connection) throws SQLException {
            final StringBuilder sb = new StringBuilder(256);
            sb.append(" and SRC_CON_UID in (");
            // We do not need CDB$ROOT and PDB$SEED
            PreparedStatement statement = connection.prepareStatement(
                    "select CON_UID from V$CONTAINERS where CON_ID > 2");
            ResultSet rs = statement.executeQuery();
            boolean first = true;
            while (rs.next()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(rs.getLong(1));
            }
            sb.append("");
            if (first) {
                return "";
            } else {
                return sb.toString() + ")";
            }

    }


}
