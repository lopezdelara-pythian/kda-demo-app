package org.example.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class Bookstore {
    @JsonProperty("bookstore_id")
    public String bookstoreId;
    public String name;
    public String city;
    public String state;

    public Bookstore() {
    }

    @Override
    public String toString() {
        return "Bookstore{" +
                "bookstoreId=" + bookstoreId +
                ", name=" + name +
                ", city=" + city +
                ", state=" + state +
                "}";
    }
}
