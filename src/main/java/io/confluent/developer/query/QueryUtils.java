package io.confluent.developer.query;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.developer.streams.SerdeUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class QueryUtils {

    private QueryUtils(){}

    public static Query<KeyValueIterator<String,JsonNode>> createRangeQuery(String lower, String upper, String jsonPredicate) {
        if (isNotBlank(jsonPredicate)) {
            return createFilteredRangeQuery(lower, upper, jsonPredicate);
        } else {
            if (isBlank(lower) && isBlank(upper)) {
                return RangeQuery.withNoBounds();
            } else if (!isBlank(lower) && isBlank(upper)) {
                return RangeQuery.withLowerBound(lower);
            } else if (isBlank(lower) && !isBlank(upper)) {
                return RangeQuery.withUpperBound(upper);
            } else {
                return RangeQuery.withRange(lower, upper);
            }
        }
    }

    public static FilteredRangeQuery<String, JsonNode> createFilteredRangeQuery(String lower, String upper, String jsonPredicate) {
        return FilteredRangeQuery.<String, JsonNode>withBounds(lower, upper)
                .predicate(jsonPredicate)
                .serdes(Serdes.String(), SerdeUtil.stockTransactionAggregateJsonNodeSerde());
    }

    public static boolean isBlank(String str) {
        return str == null || str.isBlank();
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

}
