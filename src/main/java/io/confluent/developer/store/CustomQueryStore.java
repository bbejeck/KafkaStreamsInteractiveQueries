package io.confluent.developer.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.query.CustomQuery;
import io.confluent.developer.query.CustomStoreKeyValueIterator;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CustomQueryStore extends StoreDelegate {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Configuration jsonPathConfig;
    private final Time time = Time.SYSTEM;
    private final  TypeRef<List<KeyValue<String, StockTransactionAggregationProto>>> protoTypeRef = new TypeRef<>() {};

    public CustomQueryStore(KeyValueStore<Bytes, byte[]> delegate) {
        super(delegate);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        jsonPathConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonProtobufMappingProvider())
                .build();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        if (!(query instanceof CustomQuery)) {
            return super.query(query, positionBound, config);
        } else {
            CustomQuery.Type queryType = ((CustomQuery<?>) query).type();
            return switch (queryType) {
                case MULTI_KEY -> handleMultiKeyQuery(query, positionBound, config);
                case FILTERED_RANGE -> handleFilteredRangeQuery(query, positionBound, config);
                default -> QueryResult.forUnknownQueryType(query, this);
            };
        }
    }

    private <R> QueryResult<R> handleFilteredRangeQuery(final Query<R> query,
                                                        final PositionBound positionBound,
                                                        final QueryConfig queryConfig) {
        FilteredRangeQuery<String, StockTransactionAggregationProto> filteredRangeQuery =
                (FilteredRangeQuery<String, StockTransactionAggregationProto>) query;
        Serializer<String> keySerializer = filteredRangeQuery.keySerde().serializer();
        String predicate = filteredRangeQuery.predicate();

        String lowerBound = filteredRangeQuery.lowerBound().orElse(null);
        String upperBound = filteredRangeQuery.upperBound().orElse(null);
        StringBuilder stringBuilder = new StringBuilder("[");
        try (KeyValueIterator<Bytes, byte[]> unfilteredRangeResults = range(Bytes.wrap(keySerializer.serialize(null, lowerBound)),
                Bytes.wrap(keySerializer.serialize(null, upperBound)))) {

            unfilteredRangeResults.forEachRemaining(bytesKeyValue -> {
                stringBuilder.append(new String(bytesKeyValue.value, StandardCharsets.UTF_8))
                        .append(",");
            });
            if (stringBuilder.length() > 1) {
                stringBuilder.setLength(stringBuilder.length() - 1);
            }
            stringBuilder.append("]");

            List<KeyValue<String, StockTransactionAggregationProto>> filteredJsonResults = JsonPath.using(jsonPathConfig)
                    .parse(stringBuilder.toString())
                    .read("$.[?(" + predicate + ")]", protoTypeRef);

            return (QueryResult<R>) QueryResult.forResult(new CustomStoreKeyValueIterator<>(filteredJsonResults.iterator()));
        }
    }

    private <R> QueryResult<R> handleMultiKeyQuery(final Query<R> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig queryConfig) {
        MultiKeyQuery<String, StockTransactionAggregationProto> multiKeyQuery = (MultiKeyQuery<String, StockTransactionAggregationProto>) query;
        long start = time.milliseconds();
        Set<String> keys = multiKeyQuery.keys();
        Serializer<String> keySerializer = multiKeyQuery.keySerde().serializer();
        Deserializer<StockTransactionAggregationProto> valueDeserializer = multiKeyQuery.valueSerde().deserializer();
        Set<KeyValue<String, StockTransactionAggregationProto>> results = new HashSet<>();
        keys.forEach(key -> {
            Bytes keyBytes = Bytes.wrap(keySerializer.serialize(null, key));
            byte[] returnedBytes = get(keyBytes);
            results.add(KeyValue.pair(key, valueDeserializer.deserialize(null, returnedBytes)));
        });
        long queryExecutionTime = time.milliseconds() - start;
        QueryResult<R> queryResult = (QueryResult<R>) QueryResult.forResult(new CustomStoreKeyValueIterator<>(results.iterator()));
        if (queryConfig.isCollectExecutionInfo()) {
            queryResult.addExecutionInfo(String.format("%s retrieved Total number of results [%d] in [%d] ms", multiKeyQuery.getClass(), results.size(), queryExecutionTime));
        }

        return queryResult;

    }
}
