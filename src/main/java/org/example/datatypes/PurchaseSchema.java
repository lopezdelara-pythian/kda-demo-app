package org.example.datatypes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PurchaseSchema implements DeserializationSchema<Purchase> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TypeInformation<Purchase> getProducedType() {
        return TypeInformation.of(Purchase.class);
    }

    @Override
    public boolean isEndOfStream(Purchase purchase) {
        return false;
    }

    @Override
    public Purchase deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Purchase.class);
    }
}

