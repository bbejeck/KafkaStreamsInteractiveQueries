package io.confluent.developer.store;

import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;

public class CustomInMemoryWindowStore extends InMemoryWindowStore {

    public CustomInMemoryWindowStore(final String name,
                                     final long retentionPeriod,
                                     final long windowSize,
                                     final boolean retainDuplicates,
                                     final String metricScope) {

        super(name, retentionPeriod, windowSize, retainDuplicates, metricScope);
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return super.query(query, positionBound, config);
    }
    
}
