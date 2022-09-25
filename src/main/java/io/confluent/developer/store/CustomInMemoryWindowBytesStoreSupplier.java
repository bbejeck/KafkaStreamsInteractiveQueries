package io.confluent.developer.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class CustomInMemoryWindowBytesStoreSupplier implements WindowBytesStoreSupplier {

    private final String name;
    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    public CustomInMemoryWindowBytesStoreSupplier(final String name,
                                                  final long retentionPeriod,
                                                  final long windowSize,
                                                  final boolean retainDuplicates) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public long segmentIntervalMs() {
        return 1L;
    }

    @Override
    public long windowSize() {
        return windowSize;
    }

    @Override
    public boolean retainDuplicates() {
        return retainDuplicates;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        return new CustomInMemoryWindowStore(name,
                retentionPeriod,
                windowSize,
                retainDuplicates,
                metricsScope());
    }

    @Override
    public String metricsScope() {
        return "custom-in-memory-window";
    }
}
