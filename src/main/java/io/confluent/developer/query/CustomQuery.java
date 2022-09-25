package io.confluent.developer.query;

import org.apache.kafka.streams.query.Query;

public interface CustomQuery<R> extends Query<R> {
    enum Type {
        MULTI_KEY,
        FILTERED_RANGE,
        FILTERED_WINDOW_RANGE
    }
    Type type();
}
