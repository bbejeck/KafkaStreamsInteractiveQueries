package io.confluent.developer.streams;

import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.StockTransactionAggregationResponse;
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
    private final Serde<StockTransactionAggregationResponse> stockTransactionAggregationSerde = SerdeUtil.stockTransactionAggregateSerde();
    private final Serde<ValueAndTimestamp<StockTransactionAggregationResponse>> stockTransactionAggregationValueAndTimestampSerde = SerdeUtil.valueAndTimestampSerde();

    private StockTransaction originalStockTransaction;
    private StockTransactionAggregationResponse originalStockTransactionAggregation;
    private ValueAndTimestamp<StockTransactionAggregationResponse> originalValueAndTimestamp;

    @BeforeEach
    public void setUp() {
       originalStockTransaction = StockTransaction.StockTransactionBuilder.builder().withAmount(100).withSymbol("BWB").withBuy(true).build();
       originalStockTransactionAggregation = StockTransactionAggregationResponse.newBuilder()
               .setSymbol(originalStockTransaction.getSymbol())
               .setBuys(originalStockTransaction.getAmount())
               .setSells(originalStockTransaction.getAmount()).build();
       originalValueAndTimestamp = ValueAndTimestamp.make(originalStockTransactionAggregation, currentTimestamp);
    }

    @Test
    void roundTripStockTransactionAggregateSerdeTest() {
        byte[] serialized = stockTransactionAggregationSerde.serializer().serialize(topic, originalStockTransactionAggregation);
        StockTransactionAggregationResponse deserialized = stockTransactionAggregationSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalStockTransactionAggregation);
    }

    @Test
    void roundTripStockTransactionSerdeTest() {
        byte[] serialized = stockTransactionSerde.serializer().serialize(topic, originalStockTransaction);
        StockTransaction deserialized = stockTransactionSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalStockTransaction);
    }

    @Test
    void roundTripValueAndTimestampSerdeTest() {
        byte[] serialized = stockTransactionAggregationValueAndTimestampSerde.serializer().serialize(topic, originalValueAndTimestamp);
        ValueAndTimestamp<StockTransactionAggregationResponse> deserialized = stockTransactionAggregationValueAndTimestampSerde.deserializer().deserialize(topic, serialized);
        assertEquals(deserialized, originalValueAndTimestamp);
    }
}