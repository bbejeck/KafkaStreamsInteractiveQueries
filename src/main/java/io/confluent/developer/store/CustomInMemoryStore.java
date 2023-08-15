package io.confluent.developer.store;

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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

public class CustomInMemoryStore<K, V> extends InMemoryKeyValueStore {
    private StateStoreContext context;

    private final Time time = Time.SYSTEM;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CustomInMemoryStore(String name) {
        super(name);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
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
                case MULTI_KEY -> handleMultiKeyQuery((MultiKeyQuery<K, V>) query, positionBound, config);
                case FILTERED_RANGE -> handleFilteredRangeQuery((FilteredRangeQuery<K, V>) query, positionBound, config);
                default -> QueryResult.forUnknownQueryType(query, this);
            };
        }
    }

    private <R> QueryResult<R> handleFilteredRangeQuery(final FilteredRangeQuery<K, V> filteredRangeQuery,
                                                        final PositionBound positionBound,
                                                        final QueryConfig queryConfig) {
        Serializer<K> keySerializer = filteredRangeQuery.keySerde().serializer();
        Deserializer<K> keyDeserializer = filteredRangeQuery.keySerde().deserializer();
        Deserializer<V> valueDeserializer = filteredRangeQuery.valueSerde().deserializer();
        String predicate = filteredRangeQuery.predicate();
        List<KeyValue<K, V>> allResults = new ArrayList<>();
        List<KeyValue<K, V>> filteredResults;
        K lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        K upperBound = filteredRangeQuery.upperBound().orElse(null);
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                K key = keyDeserializer.deserialize(null, bytesKeyValue.key.get());
                V value = valueDeserializer.deserialize(null, bytesKeyValue.value);

                    allResults.add(KeyValue.pair(key, value));

            });
            List<V> filteredValues = JsonPath.parse(objectMapper.writeValueAsString(allResults)).read("$.[?" + predicate + "]");
            filteredResults = filteredValues.stream().map(v -> (KeyValue<K, V>) KeyValue.pair(((JsonNode)v).get("symbol").textValue(), v)).toList();
            return (QueryResult<R>) QueryResult.forResult(new InMemoryKeyValueIterator(filteredResults.iterator()));
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

    private <R> QueryResult<R> handleMultiKeyQuery(final MultiKeyQuery<K, V> multiKeyQuery,
                                                   final PositionBound positionBound,
                                                   final QueryConfig queryConfig) {
        long start = time.milliseconds();
        Set<K> keys = multiKeyQuery.keys();
        Serializer<K> keySerializer = multiKeyQuery.keySerde().serializer();
        Deserializer<V> valueDeserializer = multiKeyQuery.valueSerde().deserializer();
        Set<KeyValue<K, V>> results = new HashSet<>();
        keys.forEach(key -> {
            Bytes keyBytes = Bytes.wrap(keySerializer.serialize(null, key));
            byte[] returnedBytes = get(keyBytes);
            results.add(KeyValue.pair(key, valueDeserializer.deserialize(null, returnedBytes)));
        });
        long queryExecutionTime = time.milliseconds() - start;
        KeyValueIterator<K, V> queryResultIterator = new InMemoryKeyValueIterator(results.iterator());
        QueryResult<R> queryResult = (QueryResult<R>) QueryResult.forResult(queryResultIterator);
        if (queryConfig.isCollectExecutionInfo()) {
            queryResult.addExecutionInfo(String.format("%s retrieved Total number of results [%d] in [%d] ms", multiKeyQuery.getClass(),results.size(),queryExecutionTime));
        }

        return queryResult;

    }


    private class InMemoryKeyValueIterator implements KeyValueIterator<K, V> {

        Iterator<KeyValue<K, V>> queryResultIterator;

        public InMemoryKeyValueIterator(Iterator<KeyValue<K, V>> queryResultIterator) {
            this.queryResultIterator = queryResultIterator;
        }

        @Override
        public void close() {
            //Do nothng
        }

        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("PeekNextKey not supported by " + CustomInMemoryStore.class);
        }

        @Override
        public boolean hasNext() {
            return queryResultIterator.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            return queryResultIterator.next();
        }
    }
}
