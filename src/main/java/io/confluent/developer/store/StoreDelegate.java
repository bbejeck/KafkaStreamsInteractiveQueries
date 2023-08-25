package io.confluent.developer.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public abstract class StoreDelegate implements KeyValueStore<Bytes, byte[]> {

    private final KeyValueStore<Bytes, byte[]> delegate;
    
    StoreDelegate(KeyValueStore<Bytes, byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        delegate.put(key, value);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        delegate.putAll(entries);
    }

    @Override
    public byte[] delete(Bytes key) {
        return delegate.delete(key);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        delegate.init(context, root);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public byte[] get(Bytes key) {
        return delegate.get(key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        return delegate.range(from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return delegate.all();
    }

    @Override
    public long approximateNumEntries() {
        return delegate.approximateNumEntries();
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        delegate.init(context, root);
    }

    @Override
    public Position getPosition() {
        return delegate.getPosition();
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query,
                                    PositionBound positionBound,
                                    QueryConfig config) {
        return delegate.query(query, positionBound, config);
    }
}
