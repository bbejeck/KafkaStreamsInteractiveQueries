package io.confluent.developer.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.confluent.developer.query.CustomQuery;
import io.confluent.developer.query.CustomStoreKeyValueIterator;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import io.confluent.developer.streams.SerdeUtil;
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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomQueryStore extends StoreDelegate {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Time time = Time.SYSTEM;

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
        FilteredRangeQuery<String, JsonNode> filteredRangeQuery =
                (FilteredRangeQuery<String, JsonNode>) query;
        Serializer<String> keySerializer = filteredRangeQuery.keySerde().serializer();
        Deserializer<String> keyDeserializer = filteredRangeQuery.keySerde().deserializer();
        Deserializer<JsonNode> valueDeserializer = SerdeUtil.stockTransactionAggregateJsonNodeSerde().deserializer();
        String predicate = filteredRangeQuery.predicate();
        Map<String, JsonNode> allResultsMap = new HashMap<>();
        List<KeyValue<String, JsonNode>> filteredResults;
        String lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        String upperBound = filteredRangeQuery.upperBound().orElse(null);
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                String key = keyDeserializer.deserialize(null, bytesKeyValue.key.get());
                JsonNode value = valueDeserializer.deserialize(null, bytesKeyValue.value);
                allResultsMap.put(key, value);
            });

            List<String> filteredKeys = JsonPath.parse(objectMapper.writeValueAsString(allResultsMap.values())).read("$.[?(" + predicate + ")].symbol");

            filteredResults = allResultsMap.entrySet().stream()
                    .filter(entry -> filteredKeys.contains(entry.getKey()))
                    .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue())).toList();

            return (QueryResult<R>) QueryResult.forResult(new CustomStoreKeyValueIterator<>(filteredResults.iterator()));
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

    private <R> QueryResult<R> handleMultiKeyQuery(final Query<R> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig queryConfig) {
        MultiKeyQuery<String, JsonNode> multiKeyQuery = (MultiKeyQuery<String, JsonNode>) query;
        long start = time.milliseconds();
        Set<String> keys = multiKeyQuery.keys();
        Serializer<String> keySerializer = multiKeyQuery.keySerde().serializer();
        Deserializer<JsonNode> valueDeserializer = multiKeyQuery.valueSerde().deserializer();
        Set<KeyValue<String, JsonNode>> results = new HashSet<>();
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

}
