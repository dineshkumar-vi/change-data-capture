package com.chassis.logminer.cdc.postgres;

import java.math.BigInteger;
import java.util.Objects;

public class PostgresLsn implements Comparable<PostgresLsn> {

    public static final PostgresLsn MAX = new PostgresLsn(BigInteger.valueOf(-2));
    public static final PostgresLsn NULL = new PostgresLsn(null);
    public static final PostgresLsn ONE = new PostgresLsn(BigInteger.valueOf(1));

    private final BigInteger lsn;

    public PostgresLsn(BigInteger lsn) {
        this.lsn = lsn;
    }

    public boolean isNull() {
        return this.lsn == null;
    }

    public static PostgresLsn valueOf(int value) {
        return new PostgresLsn(BigInteger.valueOf(value));
    }

    public static PostgresLsn valueOf(long value) {
        return new PostgresLsn(BigInteger.valueOf(value));
    }

    public static PostgresLsn valueOf(String value) {
        return new PostgresLsn(new BigInteger(value));
    }

    public long longValue() {
        return isNull() ? 0 : lsn.longValue();
    }

    public PostgresLsn add(PostgresLsn value) {
        if (isNull() && value.isNull()) {
            return PostgresLsn.NULL;
        } else if (value.isNull()) {
            return new PostgresLsn(lsn);
        } else if (isNull()) {
            return new PostgresLsn(value.lsn);
        }
        return new PostgresLsn(lsn.add(value.lsn));
    }

    public PostgresLsn subtract(PostgresLsn value) {
        if (isNull() && value.isNull()) {
            return PostgresLsn.NULL;
        } else if (value.isNull()) {
            return new PostgresLsn(lsn);
        } else if (isNull()) {
            return new PostgresLsn(value.lsn.negate());
        }
        return new PostgresLsn(lsn.subtract(value.lsn));
    }

    @Override
    public int compareTo(PostgresLsn o) {
        if (isNull() && o.isNull()) {
            return 0;
        } else if (isNull() && !o.isNull()) {
            return -1;
        } else if (!isNull() && o.isNull()) {
            return 1;
        }
        return lsn.compareTo(o.lsn);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PostgresLsn postgresLsn = (PostgresLsn) o;
        return Objects.equals(lsn, postgresLsn.lsn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lsn);
    }

    @Override
    public String toString() {
        return isNull() ? "null" : lsn.toString();
    }
}
