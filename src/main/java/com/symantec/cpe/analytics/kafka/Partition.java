package com.symantec.cpe.analytics.kafka;

import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class Partition implements Comparable<Partition> {
    Integer id;

    public Partition(Integer id) {
        Objects.requireNonNull(id, "id can't be null");
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        return id.equals(partition.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }


    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public int compareTo(Partition other) {
        return id.compareTo(other.id);
    }
}
