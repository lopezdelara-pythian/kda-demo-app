package org.example.datatypes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class BookstoreSchema implements KafkaDeserializationSchema<Bookstore> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TypeInformation<Bookstore> getProducedType() {
        return TypeInformation.of(Bookstore.class);
    }

    @Override
    public boolean isEndOfStream(Bookstore bookstore) {
        return false;
    }

    @Override
    public Bookstore deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        Bookstore bookstore = objectMapper.readValue(consumerRecord.value(), Bookstore.class);
        bookstore.bookstoreId = new String(consumerRecord.key());
        return bookstore;
    }
}
