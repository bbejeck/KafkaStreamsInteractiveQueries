package io.confluent.developer.query;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;

public class CustomStoreKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    Iterator<KeyValue<K, V>> queryResultIterator;

    public CustomStoreKeyValueIterator(Iterator<KeyValue<K, V>> queryResultIterator) {
        this.queryResultIterator = queryResultIterator;
    }

    @Override
    public void close() {
        //Do nothng
    }

    @Override
    public K peekNextKey() {
        throw new UnsupportedOperationException("PeekNextKey not supported");
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
