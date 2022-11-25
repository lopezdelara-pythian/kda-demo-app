package org.example.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class EnrichedSales {
    @JsonProperty("sales")
    public Sales sales;

    @JsonProperty("bookstore")
    public Bookstore bookstore;

    public EnrichedSales(Sales sales, Bookstore bookstore) {
        this.sales = sales;
        this.bookstore = bookstore;
    }

    @Override
    public String toString() {
        return "EnrichedSales{" +
                "sales=" + sales +
                ", bookstore=" + bookstore +
                "}";
    }
}




