package io.confluent.developer.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.streams.SerdeUtil;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JacksonProtobufMappingProviderTest {

    private final TypeRef<List<KeyValue<String, StockTransactionAggregationProto>>> protoTypeRef = new TypeRef<>() {
    };
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Configuration jsonPathConfig;

    private final List<byte[]> rangeResults = new ArrayList<>();

    private StringBuilder stringBuilder;

    private StockTransactionAggregationProto expectedProto;
    private StockTransactionAggregationProto expectedProtoII;
    private StockTransactionAggregationProto expectedProtoIII;

    @BeforeEach
    public void init() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        jsonPathConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonProtobufMappingProvider())
                .build();

        Serde<StockTransactionAggregationProto> protoSerde = SerdeUtil.stockTransactionAggregationProtoJsonSerde();
        expectedProto = StockTransactionAggregationProto.newBuilder().setSymbol("BWB").setBuys(333.0).setSells(333.0).build();
        expectedProtoII = StockTransactionAggregationProto.newBuilder().setSymbol("ALB").setBuys(400).setSells(333.0).build();
        expectedProtoIII = StockTransactionAggregationProto.newBuilder().setSymbol("EAB").setBuys(333.0).setSells(400).build();

        byte[] protoBytes = protoSerde.serializer().serialize(null, expectedProto);
        byte[] protoBytesII = protoSerde.serializer().serialize(null, expectedProtoII);
        byte[] protoBytesIII = protoSerde.serializer().serialize(null, expectedProtoIII);

        rangeResults.add(protoBytes);
        rangeResults.add(protoBytesII);
        rangeResults.add(protoBytesIII);

        stringBuilder = new StringBuilder("[");
        rangeResults.forEach(bytes -> {
            stringBuilder.append(new String(bytes, StandardCharsets.UTF_8))
                    .append(",");
        });

        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilder.append("]");

    }

    @Test
    @DisplayName("Should filter a JSON list with buys > sells")
    void shouldFilterPathTest() {
        String predicate = "@.buys > @.sells";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = List.of(KeyValue.pair(expectedProtoII.getSymbol(), expectedProtoII));
        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(stringBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should filter a JSON list with sells > buys")
    void shouldFilterWithLessThanFilter() {
        String predicate = "@.sells > @.buys";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = List.of(KeyValue.pair(expectedProtoIII.getSymbol(), expectedProtoIII));
        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(stringBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should filter a JSON list with sells == buys")
    void shouldFilterWithEqualsFilter() {
        String predicate = "@.sells == @.buys";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = List.of(KeyValue.pair(expectedProto.getSymbol(), expectedProto));
        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(stringBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should return all because each matches filter")
    void shouldFilterAllMatchingFilter() {
        String predicate = "@.buys > 299";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = List.of(KeyValue.pair(expectedProto.getSymbol(), expectedProto),
                KeyValue.pair(expectedProtoII.getSymbol(), expectedProtoII),
                KeyValue.pair(expectedProtoIII.getSymbol(), expectedProtoIII));

        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(stringBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should return an empty list when none matches")
    void shouldFilterAllNoneMatchingFilter() {
        String predicate = "@.buys > 500";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = Collections.emptyList();

        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(stringBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should handle an empty return from range")
    void shouldHandleEmptyRangeResults() {
        String predicate = "@.buys > 500";
        List<KeyValue<String, StockTransactionAggregationProto>> expected = Collections.emptyList();
        StringBuilder emptyResultsBuilder = new StringBuilder("[");
        if (emptyResultsBuilder.length() > 1) {
            emptyResultsBuilder.setLength(emptyResultsBuilder.length() - 1);
        }
        emptyResultsBuilder.append("]");
        List<KeyValue<String, StockTransactionAggregationProto>> actual = JsonPath.using(jsonPathConfig)
                .parse(emptyResultsBuilder.toString())
                .read("$.[?(" + predicate + ")]", protoTypeRef);

        assertEquals(expected, actual);
    }

}
