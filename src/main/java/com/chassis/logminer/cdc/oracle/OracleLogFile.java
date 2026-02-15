package com.chassis.logminer.cdc.oracle;

import java.math.BigInteger;
import java.util.Objects;

public class OracleLogFile {

    public enum Type {
        ARCHIVE,
        REDO
    }

    private final String fileName;
    private final OracleScn firstScn;
    private final OracleScn nextScn;
    private final BigInteger sequence;
    private final boolean current;
    private final Type type;
    private final int thread;

    public OracleLogFile(String fileName, OracleScn firstScn, OracleScn nextScn,
                         BigInteger sequence, Type type, int thread) {
        this(fileName, firstScn, nextScn, sequence, type, false, thread);
    }

    public OracleLogFile(String fileName, OracleScn firstScn, OracleScn nextScn,
                         BigInteger sequence, Type type, boolean current, int thread) {
        this.fileName = fileName;
        this.firstScn = firstScn;
        this.nextScn = nextScn;
        this.sequence = sequence;
        this.current = current;
        this.type = type;
        this.thread = thread;
    }

    public String getFileName() {
        return fileName;
    }

    public OracleScn getFirstScn() {
        return firstScn;
    }

    public OracleScn getNextScn() {
        return nextScn;
    }

    public BigInteger getSequence() {
        return sequence;
    }

    public int getThread() {
        return thread;
    }

    public boolean isCurrent() {
        return current;
    }

    public Type getType() {
        return type;
    }

    public boolean isScnInLogFileRange(OracleScn scn) {
        return getFirstScn().compareTo(scn) <= 0 &&
               (getNextScn().compareTo(scn) > 0 || getNextScn().equals(OracleScn.MAX));
    }

    @Override
    public int hashCode() {
        return Objects.hash(thread, sequence);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OracleLogFile)) {
            return false;
        }
        final OracleLogFile other = (OracleLogFile) obj;
        return thread == other.thread && Objects.equals(sequence, other.sequence);
    }

    @Override
    public String toString() {
        return "OracleLogFile{" +
                "fileName='" + fileName + '\'' +
                ", firstScn=" + firstScn +
                ", nextScn=" + nextScn +
                ", sequence=" + sequence +
                ", current=" + current +
                ", type=" + type +
                ", thread=" + thread +
                '}';
    }
}
