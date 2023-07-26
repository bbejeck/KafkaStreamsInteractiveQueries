package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SerdeUtil {

    private SerdeUtil(){};


    public static Serde<StockTransactionAggregation> stockTransactionAggregateSerde() {
        return Serdes.serdeFrom(new StockTransactionAggregationSerializer(), new StockTransactionAggregationDeserializer());
    }

    public static Serde<StockTransaction> stockTransactionSerde() {
        return Serdes.serdeFrom(new StockTransactionSerializer(), new StockTransactionDeserializer());
    }

    public static Serde<ValueAndTimestamp<StockTransactionAggregation>> valueAndTimestampSerde() {
         return Serdes.serdeFrom(new ValueAndTimestampSerializer(), new ValueAndTimestampDeserializer());
    }

    public static class ValueAndTimestampSerializer implements Serializer<ValueAndTimestamp<StockTransactionAggregation>> {
        private final LongSerializer longSerializer = new LongSerializer();
        private final StockTransactionAggregationSerializer stockTransactionAggregationSerializer = new StockTransactionAggregationSerializer();

        @Override
        public byte[] serialize(String topic, ValueAndTimestamp<StockTransactionAggregation> aggregationValueAndTimestamp) {
            final byte[] timestampBytes = longSerializer.serialize(topic, aggregationValueAndTimestamp.timestamp());
            final byte[] aggregationBytes = stockTransactionAggregationSerializer.serialize(topic, aggregationValueAndTimestamp.value());

            return ByteBuffer
                    .allocate(timestampBytes.length + aggregationBytes.length)
                    .put(timestampBytes)
                    .put(aggregationBytes)
                    .array();
        }

        @Override
        public void close() {
            longSerializer.close();
            stockTransactionAggregationSerializer.close();;
        }
    }

    public static class ValueAndTimestampDeserializer implements Deserializer<ValueAndTimestamp<StockTransactionAggregation>> {

        private final LongDeserializer longDeserializer = new LongDeserializer();
        private final StockTransactionAggregationDeserializer stockTransactionAggregationDeserializer = new StockTransactionAggregationDeserializer();

        @Override
        public ValueAndTimestamp<StockTransactionAggregation> deserialize(String topic, byte[] data) {
            final long timestamp = longDeserializer.deserialize(topic, ByteBuffer.allocate(8).put(data, 0, 8).array());
            int valueLength = data.length - 8;
            final StockTransactionAggregation stockTransactionAggregation =
                    stockTransactionAggregationDeserializer.deserialize(topic, ByteBuffer.allocate(valueLength).put(data, 8, valueLength).array());
            return ValueAndTimestamp.make(stockTransactionAggregation, timestamp);
        }
        @Override
        public void close() {
           longDeserializer.close();
           stockTransactionAggregationDeserializer.close();
        }
    }

    public static class StockTransactionAggregationSerializer implements Serializer<StockTransactionAggregation> {
        private final ObjectWriter objectWriter = new ObjectMapper().writer();

        @Override
        public byte[] serialize(String s, StockTransactionAggregation stockTransactionAggregation) {
            if (stockTransactionAggregation == null) {
                return null;
            }
            try {
                return objectWriter.writeValueAsBytes(stockTransactionAggregation);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class StockTransactionAggregationDeserializer implements Deserializer<StockTransactionAggregation> {
        private final ObjectReader objectReader = new ObjectMapper().reader();

        @Override
        public StockTransactionAggregation deserialize(String s, byte[] bytes) {
            try {
                return objectReader.readValue(bytes, StockTransactionAggregation.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class StockTransactionSerializer implements Serializer<StockTransaction> {
        private final ObjectWriter objectWriter = new ObjectMapper().writer();

        @Override
        public byte[] serialize(String s, StockTransaction stockTransaction) {
            if (stockTransaction == null) {
                return null;
            }
            try {
                return objectWriter.writeValueAsBytes(stockTransaction);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class StockTransactionDeserializer implements Deserializer<StockTransaction> {
        private final ObjectReader objectReader = new ObjectMapper().reader();

        @Override
        public StockTransaction deserialize(String s, byte[] bytes) {
            try {
                return objectReader.readValue(bytes, StockTransaction.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
