package io.confluent.developer.streams;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerdeUtilTest {

    private final String topic = "topic";
    private final long currentTimestamp = Instant.now().toEpochMilli();
    private final Serde<StockTransaction> stockTransactionSerde = SerdeUtil.stockTransactionSerde();
    private final Serde<JsonNode> stockJsonNodeSerde = SerdeUtil.stockTransactionAggregateJsonNodeSerde();
    private final Serde<ValueAndTimestamp<JsonNode>> stockTransactionJsonNodeValueAndTimestampSerde = SerdeUtil.valueAndTimestampSerde();

    private final Serde<StockTransactionAggregationProto> protoSerde = SerdeUtil.stockTransactionAggregationProtoJsonSerde();

    private final ObjectMapper mapper = new ObjectMapper();

    private StockTransaction originalStockTransaction;
    private JsonNode originalJsonNode;
    private ValueAndTimestamp<JsonNode> originalValueAndTimestamp;


    @BeforeEach
    public void setUp() throws Exception {
       originalStockTransaction = StockTransaction.StockTransactionBuilder.builder().withAmount(100).withSymbol("BWB").withBuy(true).build();
        originalJsonNode = mapper.readTree("{\"symbol\":\""+originalStockTransaction.getSymbol() +
               "\", \"buys\":"+originalStockTransaction.getAmount()+", \"sells\":" + originalStockTransaction.getAmount() + "}");
       originalValueAndTimestamp = ValueAndTimestamp.make(originalJsonNode, currentTimestamp);
    }

    @Test
    void roundTripStockTransactionJsonNodeAggregateSerdeTest() {
        byte[] serialized = stockJsonNodeSerde.serializer().serialize(topic, originalJsonNode);
        JsonNode deserialized = stockJsonNodeSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalJsonNode);
    }

    @Test
    void roundTripFromAggregationToStringToJsonNode() throws Exception {
        StockTransactionAggregation stockTransactionAggregation = new StockTransactionAggregation("BWB", 333.0, 333.0);
        StockTransactionAggregationProto proto = StockTransactionAggregationProto.newBuilder().setSymbol("BWB").setBuys(333.0).setSells(333.0).build();
        String aggregationJson = mapper.writeValueAsString(stockTransactionAggregation);
        JsonNode aggregationJsonNode = mapper.readTree(aggregationJson);

        // Kafka Streams serializing to store
        byte[] aggregationBytes = SerdeUtil.stockTransactionAggregationSerde().serializer().serialize(null, stockTransactionAggregation);
        // IQ deserializing to string for JSON work
        JsonNode rehydratedNode = SerdeUtil.stockTransactionAggregateJsonNodeSerde().deserializer().deserialize(null, aggregationBytes);
        String rehydratedJson = new String(aggregationBytes, StandardCharsets.UTF_8);
        byte[] protoBytes = JsonFormat.printer().print(proto).getBytes(StandardCharsets.UTF_8);
        

        assertEquals(aggregationJsonNode, rehydratedNode);
        assertEquals(aggregationJson, rehydratedJson);
        assertEquals(aggregationJson, mapper.readValue(aggregationBytes, JsonNode.class).toString());
        assertEquals(aggregationJson, mapper.readValue(protoBytes, JsonNode.class).toString());

    }

    @Test
    void roundTripStockTransactionSerdeTest() {
        byte[] serialized = stockTransactionSerde.serializer().serialize(topic, originalStockTransaction);
        StockTransaction deserialized = stockTransactionSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalStockTransaction);
    }

    @Test
    void roundTripValueAndTimestampSerdeTest() {
        byte[] serialized = stockTransactionJsonNodeValueAndTimestampSerde.serializer().serialize(topic, originalValueAndTimestamp);
        ValueAndTimestamp<JsonNode> deserialized = stockTransactionJsonNodeValueAndTimestampSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalValueAndTimestamp);
    }

    @Test
    void roundTripStockTransactionProtoAggregationTest() {
        StockTransactionAggregationProto expectedProto = StockTransactionAggregationProto.newBuilder().setSymbol("BWB").setBuys(333.0).setSells(333.0).build();
        byte[] protoBytes = protoSerde.serializer().serialize(topic, expectedProto);
        StockTransactionAggregationProto actualProto = protoSerde.deserializer().deserialize(topic, protoBytes);
        assertEquals(actualProto, expectedProto);
    }
}