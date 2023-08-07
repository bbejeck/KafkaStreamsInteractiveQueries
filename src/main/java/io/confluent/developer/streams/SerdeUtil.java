package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.StockTransactionAggregationResponse;
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

    public static Serde<StockTransactionAggregationResponse> stockTransactionAggregateSerde() {
        return Serdes.serdeFrom(new StockTransactionAggregationResponseSerializer(), new StockTransactionAggregationResonseDeserializer());
    }

    public static Serde<StockTransaction> stockTransactionSerde() {
        return Serdes.serdeFrom(new StockTransactionSerializer(), new StockTransactionDeserializer());
    }

    public static Serde<ValueAndTimestamp<StockTransactionAggregationResponse>> valueAndTimestampSerde() {
        return Serdes.serdeFrom(new ValueAndTimestampSerializer(), new ValueAndTimestampDeserializer());
    }

    public static class ValueAndTimestampSerializer implements Serializer<ValueAndTimestamp<StockTransactionAggregationResponse>> {
        private final LongSerializer longSerializer = new LongSerializer();
        private final StockTransactionAggregationResponseSerializer stockTransactionAggregationResponseSerializer = new StockTransactionAggregationResponseSerializer();

        @Override
        public byte[] serialize(String topic, ValueAndTimestamp<StockTransactionAggregationResponse> aggregationValueAndTimestamp) {
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

    public static class ValueAndTimestampDeserializer implements Deserializer<ValueAndTimestamp<StockTransactionAggregationResponse>> {

        private final LongDeserializer longDeserializer = new LongDeserializer();
        private final StockTransactionAggregationResonseDeserializer stockTransactionAggregationResonseDeserializer = new StockTransactionAggregationResonseDeserializer();

        @Override
        public ValueAndTimestamp<StockTransactionAggregationResponse> deserialize(String topic, byte[] data) {
            final long timestamp = longDeserializer.deserialize(topic, ByteBuffer.allocate(8).put(data, 0, 8).array());
            int valueLength = data.length - 8;
            final StockTransactionAggregationResponse stockTransactionAggregation =
                    stockTransactionAggregationResonseDeserializer.deserialize(topic, ByteBuffer.allocate(valueLength).put(data, 8, valueLength).array());
            return ValueAndTimestamp.make(stockTransactionAggregation, timestamp);
        }

        @Override
        public void close() {
            longDeserializer.close();
            stockTransactionAggregationResonseDeserializer.close();
        }
    }

    public static class StockTransactionAggregationResponseSerializer implements Serializer<StockTransactionAggregationResponse> {

        @Override
        public byte[] serialize(String s, StockTransactionAggregationResponse stockTransactionAggregation) {
            if (stockTransactionAggregation == null) {
                return null;
            }
            return stockTransactionAggregation.toByteArray();
        }
    }

    public static class StockTransactionAggregationResonseDeserializer implements Deserializer<StockTransactionAggregationResponse> {

        @Override
        public StockTransactionAggregationResponse deserialize(String s, byte[] bytes) {
            try {
                return StockTransactionAggregationResponse.parseFrom(bytes);
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
