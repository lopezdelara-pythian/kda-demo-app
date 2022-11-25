package org.example.datatypes;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class EnrichedSalesSchema implements SerializationSchema<EnrichedSales> {

    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(EnrichedSales enrichedSales) {
        try {
            return objectMapper.writeValueAsBytes(enrichedSales);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + enrichedSales, e);
        }
    }
}
