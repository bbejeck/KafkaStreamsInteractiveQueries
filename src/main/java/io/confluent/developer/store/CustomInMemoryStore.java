package io.confluent.developer.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.confluent.developer.query.CustomQuery;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomInMemoryStore extends InMemoryKeyValueStore {
    private StateStoreContext context;

    private final Time time = Time.SYSTEM;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CustomInMemoryStore(String name) {
        super(name);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        super.init(context, root);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        if (!(query instanceof CustomQuery)){
            return super.query(query, positionBound, config);
        }
        else {
            CustomQuery.Type queryType = ((CustomQuery<?>) query).type();
            return switch (queryType) {
                case MULTI_KEY -> handleMultiKeyQuery((MultiKeyQuery<String, ValueAndTimestamp<JsonNode>>) query, positionBound, config);
                case FILTERED_RANGE -> handleFilteredRangeQuery((FilteredRangeQuery<String, ValueAndTimestamp<JsonNode>>) query, positionBound, config);
                default -> QueryResult.forUnknownQueryType(query, this);
            };
        }
    }

    private <R> QueryResult<R> handleFilteredRangeQuery(final FilteredRangeQuery<String, ValueAndTimestamp<JsonNode>> filteredRangeQuery,
                                                        final PositionBound positionBound,
                                                        final QueryConfig queryConfig) {
        Serializer<String> keySerializer = filteredRangeQuery.keySerde().serializer();
        Deserializer<String> keyDeserializer = filteredRangeQuery.keySerde().deserializer();
        Deserializer<ValueAndTimestamp<JsonNode>> valueDeserializer = filteredRangeQuery.valueSerde().deserializer();
        String predicate = filteredRangeQuery.predicate();
        Map<String, ValueAndTimestamp<JsonNode>> allResultsMap = new HashMap<>();
        List<KeyValue<String, ValueAndTimestamp<JsonNode>>> filteredResults;
        String lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        String upperBound = filteredRangeQuery.upperBound().orElse(null);
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                String key = keyDeserializer.deserialize(null, bytesKeyValue.key.get());
                ValueAndTimestamp<JsonNode> value = valueDeserializer.deserialize(null, bytesKeyValue.value);
                    allResultsMap.put(key, value);
            });
            
            List<String> filteredKeys = JsonPath.parse(objectMapper.writeValueAsString(allResultsMap.values())).read("$..value[?(" + predicate + ")].symbol");

            filteredResults =  allResultsMap.entrySet().stream()
                    .filter(entry -> filteredKeys.contains(entry.getKey()))
                    .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue())).toList();

            return (QueryResult<R>) QueryResult.forResult(new InMemoryKeyValueIterator(filteredResults.iterator()));
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

    private <R> QueryResult<R> handleMultiKeyQuery(final MultiKeyQuery<String, ValueAndTimestamp<JsonNode>> multiKeyQuery,
                                                   final PositionBound positionBound,
                                                   final QueryConfig queryConfig) {
        long start = time.milliseconds();
        Set<String > keys = multiKeyQuery.keys();
        Serializer<String> keySerializer = multiKeyQuery.keySerde().serializer();
        Deserializer<ValueAndTimestamp<JsonNode>> valueDeserializer = multiKeyQuery.valueSerde().deserializer();
        Set<KeyValue<String, ValueAndTimestamp<JsonNode>>> results = new HashSet<>();
        keys.forEach(key -> {
            Bytes keyBytes = Bytes.wrap(keySerializer.serialize(null, key));
            byte[] returnedBytes = get(keyBytes);
            results.add(KeyValue.pair(key, valueDeserializer.deserialize(null, returnedBytes)));
        });
        long queryExecutionTime = time.milliseconds() - start;
        KeyValueIterator<String, ValueAndTimestamp<JsonNode>> queryResultIterator = new InMemoryKeyValueIterator(results.iterator());
        QueryResult<R> queryResult = (QueryResult<R>) QueryResult.forResult(queryResultIterator);
        if (queryConfig.isCollectExecutionInfo()) {
            queryResult.addExecutionInfo(String.format("%s retrieved Total number of results [%d] in [%d] ms", multiKeyQuery.getClass(),results.size(),queryExecutionTime));
        }

        return queryResult;

    }


    private static class InMemoryKeyValueIterator implements KeyValueIterator<String, ValueAndTimestamp<JsonNode>> {

        Iterator<KeyValue<String, ValueAndTimestamp<JsonNode>>> queryResultIterator;

        public InMemoryKeyValueIterator(Iterator<KeyValue<String , ValueAndTimestamp<JsonNode>>> queryResultIterator) {
            this.queryResultIterator = queryResultIterator;
        }

        @Override
        public void close() {
            //Do nothng
        }

        @Override
        public String peekNextKey() {
            throw new UnsupportedOperationException("PeekNextKey not supported by " + CustomInMemoryStore.class);
        }

        @Override
        public boolean hasNext() {
            return queryResultIterator.hasNext();
        }

        @Override
        public KeyValue<String, ValueAndTimestamp<JsonNode>> next() {
            return queryResultIterator.next();
        }
    }
}
