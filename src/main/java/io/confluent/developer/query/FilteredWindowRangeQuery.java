package io.confluent.developer.query;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.time.Instant;
import java.util.Optional;
import java.util.function.BiPredicate;

public class FilteredWindowRangeQuery<K, V> implements CustomQuery<KeyValueIterator<Windowed<K>, V>> {


    private final Optional<BiPredicate<K, V>> predicate;
    private final Optional<Instant> timeFrom;
    private final Optional<Instant> timeTo;

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public FilteredWindowRangeQuery(final Optional<BiPredicate<K, V>> predicate,
                              final Optional<Instant> timeFrom,
                              final Optional<Instant> timeTo,
                              final Serde<K> keySerde,
                              final Serde<V> valueSerde) {
        this.predicate = predicate;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }


    public  static <K, V> FilteredWindowRangeQuery<K, V> withPredicate(final BiPredicate<K, V> predicate) {
        return new FilteredWindowRangeQuery<>(Optional.of(predicate), Optional.empty(), Optional.empty(), null, null);
    }

    public static <K, V> FilteredWindowRangeQuery<K, V> withBounds(final Instant timeFrom, final Instant timeTo) {
        return new FilteredWindowRangeQuery<>(Optional.empty(), Optional.of(timeFrom), Optional.of(timeTo), null, null);
    }

    public FilteredWindowRangeQuery<K, V> predicate(BiPredicate<K, V> predicate){
        return new FilteredWindowRangeQuery<>(Optional.of(predicate), this.timeFrom, this.timeFrom, null, null);
    }

    public FilteredWindowRangeQuery<K,V> serdes(Serde<K> keySerde, Serde<V> valueSerde) {
        return new FilteredWindowRangeQuery<>(this.predicate, this.timeFrom, this.timeTo, keySerde, valueSerde);
    }

    public Optional<BiPredicate<K,V>> predicate() {
        return predicate;
    }

    public Optional<Instant> lowerBound() {
        return timeFrom;
    }

    public Optional<Instant> upperBound() {
        return timeTo;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    @Override
    public Type type() {
        return Type.FILTERED_WINDOW_RANGE;
    }
}
