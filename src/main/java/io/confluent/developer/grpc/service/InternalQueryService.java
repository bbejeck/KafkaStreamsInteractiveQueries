package io.confluent.developer.grpc.service;

import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.InternalQueryGrpc;
import io.confluent.developer.proto.KeyQueryMetadata;
import io.confluent.developer.proto.KeyQueryRequest;
import io.confluent.developer.proto.StockTransactionAggregationResponse;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;

@GRpcService
@Component
public class InternalQueryService extends InternalQueryGrpc.InternalQueryImplBase {

    private final KafkaStreams kafkaStreams;
    @Value("${store.name}")
    private String storeName;

    @Autowired
    public InternalQueryService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }


    @Override
    public void getAggregationForSymbol(final KeyQueryRequest request,
                                        final StreamObserver<StockTransactionAggregationResponse> responseObserver) {

        final KeyQuery<String, ValueAndTimestamp<StockTransactionAggregation>> keyQuery = KeyQuery.withKey(request.getSymbol());
        final KeyQueryMetadata keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<ValueAndTimestamp<StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(keyQuery)
                .withPartitions(partitionSet));
        final QueryResult<ValueAndTimestamp<StockTransactionAggregation>> queryResult = keyQueryResult.getOnlyPartitionResult();

        final StockTransactionAggregationResponse.Builder repsonseBuilder = StockTransactionAggregationResponse.newBuilder();
        StockTransactionAggregation aggregation = queryResult.getResult().value();
        repsonseBuilder.setSymbol(aggregation.getSymbol())
                .setBuys(aggregation.getBuys())
                .setSells(aggregation.getSells());

        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }
}
