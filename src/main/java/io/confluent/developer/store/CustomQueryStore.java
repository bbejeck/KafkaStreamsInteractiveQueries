package io.confluent.developer.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.JsonPath;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.query.CustomQuery;
import io.confluent.developer.query.CustomStoreKeyValueIterator;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class CustomQueryStore extends StoreDelegate {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Time time = Time.SYSTEM;

    private final JsonFormat.Parser parser = JsonFormat.parser();

    public CustomQueryStore(KeyValueStore<Bytes, byte[]> delegate) {
        super(delegate);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        if (!(query instanceof CustomQuery)) {
            return super.query(query, positionBound, config);
        } else {
            CustomQuery.Type queryType = ((CustomQuery<?>) query).type();
            return switch (queryType) {
                case MULTI_KEY -> handleMultiKeyQuery(query, positionBound, config);
                case FILTERED_RANGE -> handleFilteredRangeQuery(query, positionBound, config);
                default -> QueryResult.forUnknownQueryType(query, this);
            };
        }
    }

    private <R> QueryResult<R> handleFilteredRangeQuery(final Query<R> query,
                                                        final PositionBound positionBound,
                                                        final QueryConfig queryConfig) {
        FilteredRangeQuery<String, StockTransactionAggregationProto> filteredRangeQuery =
                (FilteredRangeQuery<String, StockTransactionAggregationProto>) query;
        Serializer<String> keySerializer = filteredRangeQuery.keySerde().serializer();
        Deserializer<String> keyDeserializer = filteredRangeQuery.keySerde().deserializer();
        Deserializer<StockTransactionAggregationProto> valueDeserializer = filteredRangeQuery.valueSerde().deserializer();
        String predicate = filteredRangeQuery.predicate();
        List<KeyValue<String, StockTransactionAggregationProto>> filteredResults = new ArrayList<>();
        String lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        String upperBound = filteredRangeQuery.upperBound().orElse(null);
        StringBuilder stringBuilder = new StringBuilder();
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                stringBuilder.append(new String(bytesKeyValue.value, StandardCharsets.UTF_8));
            });

            List<String> filteredJsonResults = JsonPath.parse(stringBuilder.toString()).read("$.[?(" + predicate + ")]", List.class);
            StockTransactionAggregationProto.Builder builder = StockTransactionAggregationProto.newBuilder();

            filteredResults = filteredJsonResults.stream()
                    .map(json -> {
                        StockTransactionAggregationProto agg = fromLinkedHashMap(json, builder);
                        return KeyValue.pair(agg.getSymbol(), agg);
                    })
                    .toList();

            return (QueryResult<R>) QueryResult.forResult(new CustomStoreKeyValueIterator<>(filteredResults.iterator()));
        }
    }

    private <R> QueryResult<R> handleMultiKeyQuery(final Query<R> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig queryConfig) {
        MultiKeyQuery<String, StockTransactionAggregationProto> multiKeyQuery = (MultiKeyQuery<String, StockTransactionAggregationProto>) query;
        long start = time.milliseconds();
        Set<String> keys = multiKeyQuery.keys();
        Serializer<String> keySerializer = multiKeyQuery.keySerde().serializer();
        Deserializer<StockTransactionAggregationProto> valueDeserializer = multiKeyQuery.valueSerde().deserializer();
        Set<KeyValue<String, StockTransactionAggregationProto>> results = new HashSet<>();
        keys.forEach(key -> {
            Bytes keyBytes = Bytes.wrap(keySerializer.serialize(null, key));
            byte[] returnedBytes = get(keyBytes);
            results.add(KeyValue.pair(key, valueDeserializer.deserialize(null, returnedBytes)));
        });
        long queryExecutionTime = time.milliseconds() - start;
        QueryResult<R> queryResult = (QueryResult<R>) QueryResult.forResult(new CustomStoreKeyValueIterator<>(results.iterator()));
        if (queryConfig.isCollectExecutionInfo()) {
            queryResult.addExecutionInfo(String.format("%s retrieved Total number of results [%d] in [%d] ms", multiKeyQuery.getClass(), results.size(), queryExecutionTime));
        }

        return queryResult;

    }

    private StockTransactionAggregationProto fromLinkedHashMap(final String json, final StockTransactionAggregationProto.Builder builder) {
        try {

             json.forEach((key, value) -> builder.setField());
             StockTransactionAggregationProto aggregationProto = builder.build();
             builder.clear();
            return aggregationProto;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

}
