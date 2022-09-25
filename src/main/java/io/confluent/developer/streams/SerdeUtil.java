package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class SerdeUtil {


    public static Serde<StockTransactionAggregation> stockTransactionAggregateSerde() {
        return Serdes.serdeFrom(new StockTransactionAggregationSerializer(), new StockTransactionAggregationDeserializer());
    }

    public static Serde<StockTransaction> stockTransactionSerde() {
        return Serdes.serdeFrom(new StockTransactionSerializer(), new StockTransactionDeserializer());
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
