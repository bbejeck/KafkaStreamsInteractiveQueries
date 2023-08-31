package io.confluent.developer.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class CustomPersistentStoreSupplier implements KeyValueBytesStoreSupplier {

    private final StoreSupplier<KeyValueStore<Bytes, byte[]>> innerSupplier;

    public CustomPersistentStoreSupplier(StoreSupplier<KeyValueStore<Bytes, byte[]>> innerSupplier) {
        this.innerSupplier = innerSupplier;
    }

    @Override
    public String name() {
        return innerSupplier.name();
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new CustomQueryStore(innerSupplier.get());
    }

    @Override
    public String metricsScope() {
        return "custom-query-" + innerSupplier.metricsScope();
    }

}
