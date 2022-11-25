package org.example.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Sales {
    @JsonProperty("bookstore_id")
    public String bookstoreId;

    @JsonProperty("books_sold")
    public long booksSold;

    @JsonProperty("sales_amount")
    public long salesAmount;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonProperty("window_start")
    public Date windowStart;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonProperty("window_end")
    public Date windowEnd;

    public Sales() {
    }

    public Sales(String bookstoreId, long booksSold, long salesAmount, Date windowStart, Date windowEnd) {
        this.bookstoreId = bookstoreId;
        this.booksSold = booksSold;
        this.salesAmount = salesAmount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "Sales{" +
                "bookstoreId=" + bookstoreId +
                ", booksSold=" + booksSold +
                ", salesAmount=" + salesAmount +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                "}";
    }
}
