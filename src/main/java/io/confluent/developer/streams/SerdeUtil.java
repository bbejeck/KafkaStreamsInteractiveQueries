package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.confluent.developer.model.StockTransaction;
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

    private SerdeUtil() {
    }

    public static Serde<JsonNode> stockTransactionAggregateSerde() {
        return Serdes.serdeFrom(new JsonNodeSerializer(), new JsonNodeDeserializer());
    }

    public static Serde<StockTransaction> stockTransactionSerde() {
        return Serdes.serdeFrom(new StockTransactionSerializer(), new StockTransactionDeserializer());
    }

    public static Serde<ValueAndTimestamp<JsonNode>> valueAndTimestampSerde() {
        return Serdes.serdeFrom(new ValueAndTimestampSerializer(), new ValueAndTimestampDeserializer());
    }

    public static class ValueAndTimestampSerializer implements Serializer<ValueAndTimestamp<JsonNode>> {
        private final LongSerializer longSerializer = new LongSerializer();
        private final JsonNodeSerializer stockTransactionAggregationResponseSerializer = new JsonNodeSerializer();

        @Override
        public byte[] serialize(String topic, ValueAndTimestamp<JsonNode> aggregationValueAndTimestamp) {
            final byte[] timestampBytes = longSerializer.serialize(topic, aggregationValueAndTimestamp.timestamp());
            final byte[] aggregationBytes = stockTransactionAggregationResponseSerializer.serialize(topic, aggregationValueAndTimestamp.value());

            return ByteBuffer
                    .allocate(timestampBytes.length + aggregationBytes.length)
                    .put(timestampBytes)
                    .put(aggregationBytes)
                    .array();
        }

        @Override
        public void close() {
            longSerializer.close();
            stockTransactionAggregationResponseSerializer.close();
        }
    }

    public static class ValueAndTimestampDeserializer implements Deserializer<ValueAndTimestamp<JsonNode>> {

        private final LongDeserializer longDeserializer = new LongDeserializer();
        private final JsonNodeDeserializer jsonNodeDeserializer = new JsonNodeDeserializer();

        @Override
        public ValueAndTimestamp<JsonNode> deserialize(String topic, byte[] data) {
            final long timestamp = longDeserializer.deserialize(topic, ByteBuffer.allocate(8).put(data, 0, 8).array());
            int valueLength = data.length - 8;
            final JsonNode stockTransactionAggregation =
                    jsonNodeDeserializer.deserialize(topic, ByteBuffer.allocate(valueLength).put(data, 8, valueLength).array());
            return ValueAndTimestamp.make(stockTransactionAggregation, timestamp);
        }

        @Override
        public void close() {
            longDeserializer.close();
            jsonNodeDeserializer.close();
        }
    }

    public static class JsonNodeSerializer implements Serializer<JsonNode> {
        ObjectWriter writer = new ObjectMapper().writer();

        @Override
        public byte[] serialize(String s, JsonNode stockTransactionAggregation) {
            if (stockTransactionAggregation == null) {
                return null;
            }
            try {
                return writer.writeValueAsBytes(stockTransactionAggregation);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JsonNodeDeserializer implements Deserializer<JsonNode> {
             ObjectMapper mapper = new ObjectMapper();
        @Override
        public JsonNode deserialize(String s, byte[] bytes) {
            try {
                return mapper.readTree(bytes);
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
