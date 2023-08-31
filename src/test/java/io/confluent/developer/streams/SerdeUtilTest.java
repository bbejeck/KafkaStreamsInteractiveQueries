package io.confluent.developer.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerdeUtilTest {

    private final String topic = "topic";
    private final long currentTimestamp = Instant.now().toEpochMilli();
    private final Serde<StockTransaction> stockTransactionSerde = SerdeUtil.stockTransactionSerde();
    private final Serde<JsonNode> stockJsonNodeSerde = SerdeUtil.stockTransactionAggregateJsonNodeSerde();
    private final Serde<ValueAndTimestamp<JsonNode>> stockTransactionJsonNodeValueAndTimestampSerde = SerdeUtil.valueAndTimestampSerde();

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
        String aggregationJson = mapper.writeValueAsString(stockTransactionAggregation);
        JsonNode aggregationJsonNode = mapper.readTree(aggregationJson);

        // Kafka Streams serializing to store
        byte[] aggregationBytes = SerdeUtil.stockTransactionAggregationSerde().serializer().serialize(null, stockTransactionAggregation);
        // IQ deserializing to string for JSON work
        JsonNode rehydratedNode = SerdeUtil.stockTransactionAggregateJsonNodeSerde().deserializer().deserialize(null, aggregationBytes);
        
        assertEquals(rehydratedNode, aggregationJsonNode);

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
}