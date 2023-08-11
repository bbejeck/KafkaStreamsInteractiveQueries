package io.confluent.developer.grpc.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.developer.proto.InternalQueryGrpc;
import io.confluent.developer.proto.KeyQueryMetadataProto;
import io.confluent.developer.proto.KeyQueryRequestProto;
import io.confluent.developer.proto.MultKeyQueryRequestProto;
import io.confluent.developer.proto.MultipleResultProto;
import io.confluent.developer.proto.QueryResponseProto;
import io.confluent.developer.proto.RangeQueryRequestProto;
import io.confluent.developer.proto.SingleResultProto;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import io.confluent.developer.streams.SerdeUtil;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@GRpcService
@Component
public class InternalQueryService extends InternalQueryGrpc.InternalQueryImplBase {

    private final KafkaStreams kafkaStreams;
    @Value("${store.name}")
    private String storeName;
    private Serde<String> stringSerde;
    private Serde<ValueAndTimestamp<JsonNode>> valueAndTimestampSerde;

    @Autowired
    public InternalQueryService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @PostConstruct
    public void init() {
        stringSerde = Serdes.String();
        valueAndTimestampSerde = SerdeUtil.valueAndTimestampSerde();
    }


    @Override
    public void getAggregationForSymbol(final KeyQueryRequestProto request,
                                        final StreamObserver<QueryResponseProto> responseObserver) {

        final KeyQuery<String, ValueAndTimestamp<JsonNode>> keyQuery = KeyQuery.withKey(request.getSymbol());
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<ValueAndTimestamp<JsonNode>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(keyQuery)
                .withPartitions(partitionSet));
        final QueryResult<ValueAndTimestamp<JsonNode>> queryResult = keyQueryResult.getOnlyPartitionResult();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();
        JsonNode aggregation = queryResult.getResult().value();
        repsonseBuilder.setSingleResult(SingleResultProto.newBuilder().setJsonResult(aggregation.textValue()).build());
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
   }

    @Override
    public void rangeQueryForSymbols(RangeQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final FilteredRangeQuery<String, ValueAndTimestamp<JsonNode>> filteredRangeQuery = FilteredRangeQuery
                .<String, ValueAndTimestamp<JsonNode>>withBounds(request.getLower(), request.getUpper())
                .predicate(request.getJsonPredicate())
                .serdes(stringSerde, valueAndTimestampSerde);


        final StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(filteredRangeQuery));
        final Map<Integer,QueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>>> allPartitionResults = keyQueryResult.getPartitionResults();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();

        MultipleResultProto.Builder builder = MultipleResultProto.newBuilder();
        allPartitionResults.forEach((k,v) -> {
            var keyValues = v.getResult();
            keyValues.forEachRemaining(kv -> {
                long timestamp = kv.value.timestamp();
                JsonNode node = kv.value.value();
                ((ObjectNode) node).put("timestamp", timestamp);
                builder.addJsonResults(node.textValue());
            });
        });
        repsonseBuilder.setMultipleResults(builder.build());
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAggregationsForSymbols(MultKeyQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final MultiKeyQuery<String, ValueAndTimestamp<JsonNode>> multiKeyQuery = MultiKeyQuery.withKeys(new HashSet<>(request.getSymbolsList()));
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(multiKeyQuery)
                .withPartitions(partitionSet));
        final QueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> queryResult = keyQueryResult.getOnlyPartitionResult();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();
        KeyValueIterator<String, ValueAndTimestamp<JsonNode>> aggregations = queryResult.getResult();
        MultipleResultProto.Builder builder = MultipleResultProto.newBuilder();
        aggregations.forEachRemaining(kv -> {
            JsonNode node = kv.value.value();
            long timestamp = kv.value.timestamp();
            ((ObjectNode)node).put("timestamp", timestamp);
            builder.addJsonResults(node.textValue());
        });
        repsonseBuilder.setMultipleResults(builder.build());
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }
}
