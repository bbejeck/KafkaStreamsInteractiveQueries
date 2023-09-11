package io.confluent.developer.query;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Optional;

public class FilteredRangeQuery<K, V> implements CustomQuery<KeyValueIterator<K, V>> {

    private final String predicate;
    private final Optional<K> lowerBound;
    private final Optional<K> upperBound;

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private FilteredRangeQuery(final String predicate,
                              final Optional<K> lowerBound,
                              final Optional<K> upperBound,
                              final Serde<K> keySerde,
                              final Serde<V> valueSerde) {
        this.predicate = predicate;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }


    public  static <K, V> FilteredRangeQuery<K, V> withPredicate(final String jsonPredicate) {
        return new FilteredRangeQuery<>(jsonPredicate, Optional.empty(), Optional.empty(), null, null);
    }

    public static <K, V> FilteredRangeQuery<K, V> withBounds(final K lowerBound, final K upperBound) {
        return new FilteredRangeQuery<>(null, Optional.ofNullable(lowerBound), Optional.ofNullable(upperBound), null, null);
    }

    public FilteredRangeQuery<K, V> predicate(String jsonPredicate){
        return new FilteredRangeQuery<>(jsonPredicate, this.lowerBound, this.upperBound, null, null);
    }

    public FilteredRangeQuery<K,V> serdes(Serde<K> keySerde, Serde<V> valueSerde) {
        return new FilteredRangeQuery<>(this.predicate, this.lowerBound, this.upperBound, keySerde, valueSerde);
    }

    public String predicate() {
        return predicate;
    }

    public Optional<K> lowerBound() {
        return lowerBound;
    }

    public Optional<K> upperBound() {
        return upperBound;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }
    
    @Override
    public Type type() {
        return Type.FILTERED_RANGE;
    }
}
