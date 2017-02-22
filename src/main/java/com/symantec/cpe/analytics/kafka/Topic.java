package com.symantec.cpe.analytics.kafka;

import java.util.Objects;

/**
 * @author Brandon Kearby
 *         February 16 2017.
 */
public class Topic {
    private String name;

    public Topic(String name) {
        Objects.requireNonNull(name, "name can't be null");
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topic topic = (Topic) o;

        return name.equals(topic.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }


    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }
}
