package io.confluent.developer.store;

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
    private boolean isWrappedByTimestampedStore;

    private final Time time = Time.SYSTEM;

    public CustomInMemoryStore(String name) {
        super(name);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        isWrappedByTimestampedStore = root instanceof TimestampedKeyValueStore;
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
        BiPredicate<K, V> predicate = filteredRangeQuery.predicate();
        List<KeyValue<K, V>> filteredResults = new ArrayList<>();
        K lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        K upperBound = filteredRangeQuery.upperBound().orElse(null);
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                K key = keyDeserializer.deserialize(null, bytesKeyValue.key.get());
                byte[] valueBytes = isWrappedByTimestampedStore ?
                        Arrays.copyOfRange(bytesKeyValue.value, 8, bytesKeyValue.value.length)
                        : bytesKeyValue.value;
                V value = valueDeserializer.deserialize(null, valueBytes);
                if (predicate.test(key, value)) {
                    filteredResults.add(KeyValue.pair(key, value));
                }
            });
            return (QueryResult<R>) QueryResult.forResult(new InMemoryKeyValueIterator(filteredResults.iterator()));
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
            byte[] valueBytes;
            if (isWrappedByTimestampedStore) {
                valueBytes = Arrays.copyOfRange(returnedBytes, 8, returnedBytes.length);
            } else {
                valueBytes = returnedBytes;
            }
            results.add(KeyValue.pair(key, valueDeserializer.deserialize(null, valueBytes)));
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
