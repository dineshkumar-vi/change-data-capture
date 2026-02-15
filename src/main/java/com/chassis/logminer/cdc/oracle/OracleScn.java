package com.chassis.logminer.cdc.oracle;

import java.math.BigInteger;
import java.util.Objects;

public class OracleScn implements Comparable<OracleScn> {

    public static final OracleScn MAX = new OracleScn(BigInteger.valueOf(-2));
    public static final OracleScn NULL = new OracleScn(null);
    public static final OracleScn ONE = new OracleScn(BigInteger.valueOf(1));

    private final BigInteger scn;

    public OracleScn(BigInteger scn) {
        this.scn = scn;
    }

    public boolean isNull() {
        return this.scn == null;
    }

    public static OracleScn valueOf(int value) {
        return new OracleScn(BigInteger.valueOf(value));
    }

    public static OracleScn valueOf(long value) {
        return new OracleScn(BigInteger.valueOf(value));
    }

    public static OracleScn valueOf(String value) {
        return new OracleScn(new BigInteger(value));
    }

    public long longValue() {
        return isNull() ? 0 : scn.longValue();
    }

    public OracleScn add(OracleScn value) {
        if (isNull() && value.isNull()) {
            return OracleScn.NULL;
        } else if (value.isNull()) {
            return new OracleScn(scn);
        } else if (isNull()) {
            return new OracleScn(value.scn);
        }
        return new OracleScn(scn.add(value.scn));
    }

    public OracleScn subtract(OracleScn value) {
        if (isNull() && value.isNull()) {
            return OracleScn.NULL;
        } else if (value.isNull()) {
            return new OracleScn(scn);
        } else if (isNull()) {
            return new OracleScn(value.scn.negate());
        }
        return new OracleScn(scn.subtract(value.scn));
    }

    @Override
    public int compareTo(OracleScn o) {
        if (isNull() && o.isNull()) {
            return 0;
        } else if (isNull() && !o.isNull()) {
            return -1;
        } else if (!isNull() && o.isNull()) {
            return 1;
        }
        return scn.compareTo(o.scn);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OracleScn oracleScn = (OracleScn) o;
        return Objects.equals(scn, oracleScn.scn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scn);
    }

    @Override
    public String toString() {
        return isNull() ? "null" : scn.toString();
    }
}
