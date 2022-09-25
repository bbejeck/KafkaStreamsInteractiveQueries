package io.confluent.developer.store;

import java.time.Duration;
import java.util.Objects;

public class CustomStores {

    public static CustomInMemoryWindowBytesStoreSupplier customInMemoryWindowStore(final String name,
                                                                                   final Duration retentionPeriod,
                                                                                   final Duration windowSize,
                                                                                   final boolean retainDuplicates) throws IllegalArgumentException {
        Objects.requireNonNull(name, "name can't be null");

        if (retentionPeriod.isNegative()) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }

        if (windowSize.isNegative()) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }

        if (windowSize.toMillis() > retentionPeriod.toMillis()) {
            throw new IllegalArgumentException("The retention period of the window store "
                    + name + " must be no smaller than its window size. Got size=["
                    + windowSize + "], retention=[" + retentionPeriod + "]");
        }

        return new CustomInMemoryWindowBytesStoreSupplier(name, retentionPeriod.toMillis(), windowSize.toMillis(), retainDuplicates);
    }

    public static CustomInMemoryBytesStoreSupplier customInMemoryBytesStoreSupplier(final String name) {
        return new CustomInMemoryBytesStoreSupplier(name);
    }
}
