package com.chassis.logminer.cdc.postgres;

import java.util.Objects;

public class PostgresWalFile {

    private final String name;
    private final String location;
    private final long size;

    public PostgresWalFile(String name, String location, long size) {
        this.name = name;
        this.location = location;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostgresWalFile that = (PostgresWalFile) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "PostgresWalFile{" +
                "name='" + name + '\'' +
                ", location='" + location + '\'' +
                ", size=" + size +
                '}';
    }
}
