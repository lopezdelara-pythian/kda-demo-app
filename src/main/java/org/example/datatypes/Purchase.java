package org.example.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Purchase {
    @JsonProperty("book_id")
    public String bookId;

    @JsonProperty("bookstore_id")
    public String bookstoreId;

    public String customer;
    public int quantity;

    @JsonProperty("unit_price")
    public int unitPrice;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "EEE MMM dd HH:mm:ss z yyyy", locale = "en_US")
    public Date timestamp;

    public Purchase() {
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "bookId=" + bookId +
                ", bookstoreId=" + bookstoreId +
                ", customer=" + customer +
                ", quantity=" + quantity +
                ", unitPrice=" + unitPrice +
                ", timestamp=" + timestamp +
                "}";
    }
}

