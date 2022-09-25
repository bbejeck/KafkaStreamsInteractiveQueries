package io.confluent.developer.query;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Optional;
import java.util.function.BiPredicate;

public class FilteredRangeQuery<K, V> implements CustomQuery<KeyValueIterator<K, V>> {

    private final BiPredicate<K, V> predicate;
    private final Optional<K> lowerBound;
    private final Optional<K> upperBound;

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private FilteredRangeQuery(final BiPredicate<K, V> predicate,
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


    public  static <K, V> FilteredRangeQuery<K, V> withPredicate(final BiPredicate<K, V> predicate) {
        return new FilteredRangeQuery<>(predicate, Optional.empty(), Optional.empty(), null, null);
    }

    public static <K, V> FilteredRangeQuery<K, V> withBounds(final K lowerBound, final K upperBound) {
        return new FilteredRangeQuery<>(null, Optional.of(lowerBound), Optional.of(upperBound), null, null);
    }

    public FilteredRangeQuery<K, V> predicate(BiPredicate<K, V> predicate){
        return new FilteredRangeQuery<>(predicate, this.lowerBound, this.upperBound, null, null);
    }

    public FilteredRangeQuery<K,V> serdes(Serde<K> keySerde, Serde<V> valueSerde) {
        return new FilteredRangeQuery<>(this.predicate, this.lowerBound, this.upperBound, keySerde, valueSerde);
    }

    public BiPredicate<K,V> predicate() {
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
