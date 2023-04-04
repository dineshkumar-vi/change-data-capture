package com.chassis.logminer.cdc.core;

public class LogmnrContents {

    public static final short INTERNAL = 0;
    public static final short INSERT = 1;
    public static final short DELETE = 2;
    public static final short UPDATE = 3;
    public static final short DDL = 5;
    public static final short START = 6;
    public static final short COMMIT = 7;
    public static final short ROLLBACK = 36;
    public static final short SELECT_LOB_LOCATOR = 9;
    public static final short LOB_WRITE = 10;
    public static final short LOB_TRIM = 11;
    public static final short LOB_ERASE = 29;
    public static final short XML_DOC_BEGIN = 68;
    public static final short XML_DOC_WRITE = 70;
    public static final short XML_DOC_END = 71;

}
