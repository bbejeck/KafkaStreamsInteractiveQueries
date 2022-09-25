package io.confluent.developer.query;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Collections;
import java.util.Set;

public class MultiKeyQuery<K, V> implements CustomQuery<KeyValueIterator<K, V>> {

    private final Set<K> keys;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private MultiKeyQuery(final Set<K> keys,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde) {
        this.keys = keys;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Type type() {
        return Type.MULTI_KEY;
    }

    public static <K, V> MultiKeyQuery<K, V> withKeys(final Set<K> keys) {
        return new MultiKeyQuery<>(keys, null, null);
    }

    public MultiKeyQuery<K, V> keySerde(final Serde<K> keySerde ) {
        return new MultiKeyQuery<>(this.keys, keySerde, this.valueSerde);
    }

    public MultiKeyQuery<K, V> valueSerde(final Serde<V> valueSerde) {
        return new MultiKeyQuery<>(this.keys, this.keySerde, valueSerde);
    }

    public Set<K> keys() {
        return Collections.unmodifiableSet(keys);
    }

    public Serde<K> keySerde() {
        return this.keySerde;
    }

    public Serde<V> valueSerde() {
        return this.valueSerde;
    }
}
