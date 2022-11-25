package org.example.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.datatypes.Bookstore;
import org.example.datatypes.EnrichedSales;
import org.example.datatypes.Sales;

public class EnrichSales extends CoProcessFunction<Sales, Bookstore,
        EnrichedSales> {

    // bookstores reference data
    private ValueState<Bookstore> bookstores = null;

    @Override
    public void open(Configuration config) {
        // initialize the state store
        ValueStateDescriptor<Bookstore> descriptor = new ValueStateDescriptor<>(
                "bookstores",
                TypeInformation.of(Bookstore.class)
        );
        bookstores = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(Sales sales,
                                Context context,
                                Collector<EnrichedSales> out) throws Exception {
        // get the bookstore info for the current bookstoreId
        out.collect(new EnrichedSales(sales, bookstores.value()));
    }

    @Override
    public void processElement2(
            Bookstore bookstore,
            Context context,
            Collector<EnrichedSales> collector) throws Exception {
        // add or update a bookstore to the state store
        bookstores.update(bookstore);
    }
}
