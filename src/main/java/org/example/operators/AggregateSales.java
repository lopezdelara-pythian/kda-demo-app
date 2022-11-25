package org.example.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.datatypes.Purchase;

public class AggregateSales implements AggregateFunction<Purchase, SalesAccumulator,
        SalesAccumulator> {

    @Override
    public SalesAccumulator createAccumulator() {
        return new SalesAccumulator();
    }

    @Override
    public SalesAccumulator add(Purchase purchase, SalesAccumulator accumulator) {
        accumulator.booksSold += purchase.quantity;
        accumulator.salesAmount += (long) purchase.quantity * purchase.unitPrice;
        return accumulator;
    }

    @Override
    public SalesAccumulator getResult(SalesAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public SalesAccumulator merge(SalesAccumulator a, SalesAccumulator b) {
        a.booksSold += b.booksSold;
        a.salesAmount += b.salesAmount;
        return a;
    }
}
