package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class SerdeUtil {

    private SerdeUtil() {
    }

    public static Serde<JsonNode> stockTransactionAggregateJsonNodeSerde() {
        return Serdes.serdeFrom(new ObjectSerializer<>(), new ObjectDeserializer<>(JsonNode.class));
    }

    public static Serde<StockTransaction> stockTransactionSerde() {
        return Serdes.serdeFrom(new ObjectSerializer<>(), new ObjectDeserializer<>(StockTransaction.class));
    }

    public static Serde<ValueAndTimestamp<JsonNode>> valueAndTimestampSerde() {
        return Serdes.serdeFrom(new ValueAndTimestampSerializer(), new ValueAndTimestampDeserializer());
    }

    public static Serde<StockTransactionAggregation> stockTransactionAggregationSerde() {
        return Serdes.serdeFrom(new ObjectSerializer<>(), new ObjectDeserializer<>(StockTransactionAggregation.class));
    }

    public static Serde<StockTransactionAggregationProto> stockTransactionAggregationProtoJsonSerde() {
        return Serdes.serdeFrom(new ProtoAggregationJsonSerializer<>(),
                new ProtoAggregationDeserializer<>(StockTransactionAggregationProto::newBuilder));
    }

    public static class ValueAndTimestampSerializer implements Serializer<ValueAndTimestamp<JsonNode>> {
        private final LongSerializer longSerializer = new LongSerializer();
        private final ObjectSerializer<JsonNode> stockTransactionAggregationResponseSerializer = new ObjectSerializer<>();

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
        private final ObjectDeserializer<JsonNode> jsonNodeDeserializer = new ObjectDeserializer<>(JsonNode.class);

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

    public static class ProtoAggregationJsonSerializer<T extends Message> implements Serializer<T> {

        private final JsonFormat.Printer printer = JsonFormat.printer();
        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return printer.print(data).getBytes(StandardCharsets.UTF_8);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ProtoAggregationDeserializer<T extends Message> implements Deserializer<T> {
        private final JsonFormat.Parser parser = JsonFormat.parser();

        private final Supplier<Message.Builder> builderSupplier;

        public ProtoAggregationDeserializer(Supplier<Message.Builder> builderSupplier) {
            this.builderSupplier = builderSupplier;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            String json = new String(data, StandardCharsets.UTF_8);
            try {
                Message.Builder builder = builderSupplier.get();
                parser.merge(json, builder);
                return (T) builder.build();
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ObjectSerializer<T> implements Serializer<T> {
        private final ObjectWriter objectWriter = new ObjectMapper().writer();

        @Override
        public byte[] serialize(String s, T object) {
            if (object == null) {
                return null;
            }
            try {
                return objectWriter.writeValueAsBytes(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ObjectDeserializer<T> implements Deserializer<T> {
        private final ObjectReader objectReader = new ObjectMapper().reader();
        private final Class<T>  classToDeserialze;

        public ObjectDeserializer(Class<T> classToDeserialze) {
            this.classToDeserialze = classToDeserialze;
        }

        @Override
        public T deserialize(String s, byte[] bytes) {
            try {
                return objectReader.readValue(bytes, classToDeserialze);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
