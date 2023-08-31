package io.confluent.developer.store;

import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public class CustomQueryStores {
    

    private CustomQueryStores(){}
    public static KeyValueBytesStoreSupplier customInMemoryBytesStoreSupplier(final String name) {
        return new CustomInMemoryBytesStoreSupplier(Stores.inMemoryKeyValueStore(name));
    }

    public static CustomPersistentStoreSupplier customPersistentStoreSupplier(final String name) {
        return new CustomPersistentStoreSupplier(Stores.persistentKeyValueStore(name));
    }
}
