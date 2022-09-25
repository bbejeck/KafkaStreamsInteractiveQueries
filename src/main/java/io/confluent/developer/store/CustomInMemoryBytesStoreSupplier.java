package io.confluent.developer.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;

public class CustomInMemoryBytesStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;

    public CustomInMemoryBytesStoreSupplier(final String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new CustomInMemoryStore(name);
    }

    @Override
    public String metricsScope() {
        return "custom-in-memory";
    }
}
