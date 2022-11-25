package org.example.operators;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.datatypes.Sales;

import java.util.Date;

public class CollectSales extends ProcessWindowFunction<SalesAccumulator, Sales, String, TimeWindow> {

    @Override
    public void process(
            final String bookstoreId,
            final Context context,
            final Iterable<SalesAccumulator> elements,
            final Collector<Sales> out) throws Exception {

        SalesAccumulator acc = elements.iterator().next();
        Date start = new Date(context.window().getStart());
        Date end = new Date(context.window().getEnd());

        out.collect(new Sales(bookstoreId, acc.booksSold, acc.salesAmount, start, end));
    }
}
