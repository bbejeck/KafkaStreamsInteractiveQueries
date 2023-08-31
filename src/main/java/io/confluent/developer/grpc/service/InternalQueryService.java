package io.confluent.developer.grpc.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.InternalQueryGrpc;
import io.confluent.developer.proto.KeyQueryMetadataProto;
import io.confluent.developer.proto.KeyQueryRequestProto;
import io.confluent.developer.proto.MultKeyQueryRequestProto;
import io.confluent.developer.proto.QueryResponseProto;
import io.confluent.developer.proto.RangeQueryRequestProto;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.query.MultiKeyQuery;
import io.confluent.developer.query.QueryUtils;
import io.confluent.developer.streams.SerdeUtil;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@GRpcService
@Component
public class InternalQueryService extends InternalQueryGrpc.InternalQueryImplBase {

    private final KafkaStreams kafkaStreams;
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Value("${store.name}")
    String storeName;
    // protected scope for testing
    Serde<String> stringSerde;
    // protected scope for testing



    @Autowired
    public InternalQueryService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @PostConstruct
    public void init() {
        stringSerde = Serdes.String();
    }


    @Override
    public void keyQueryService(final KeyQueryRequestProto request,
                                final StreamObserver<QueryResponseProto> responseObserver) {

        final KeyQuery<String, StockTransactionAggregation> keyQuery = KeyQuery.withKey(request.getSymbol());
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<StockTransactionAggregation> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(keyQuery)
                .withPartitions(partitionSet));
        final QueryResult<StockTransactionAggregation> queryResult = keyQueryResult.getOnlyPartitionResult();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();
        StockTransactionAggregation aggregation = queryResult.getResult();
        repsonseBuilder.addAllExecutionInfo(queryResult.getExecutionInfo());
        StockTransactionAggregationProto.Builder builder = StockTransactionAggregationProto.newBuilder();
        repsonseBuilder.addAggregations(builder.setSymbol(aggregation.getSymbol())
                .setBuys(aggregation.getBuys())
                .setSells(aggregation.getSells()));
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rangeQueryService(RangeQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final Query<KeyValueIterator<String, StockTransactionAggregation>> rangeQuery =
                QueryUtils.createRangeQuery(request.getLower(), request.getUpper(), request.getPredicate());

        final StateQueryResult<KeyValueIterator<String, StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(rangeQuery)
                .withPartitions(new HashSet<>(request.getPartitionsList())));
        final Map<Integer, QueryResult<KeyValueIterator<String, StockTransactionAggregation>>> allPartitionResults = keyQueryResult.getPartitionResults();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();

        List<StockTransactionAggregationProto> remoteRangeResults = new ArrayList<>();
        StockTransactionAggregationProto.Builder builder = StockTransactionAggregationProto.newBuilder();
        allPartitionResults.forEach((k, v) -> {
            var keyValues = v.getResult();
            keyValues.forEachRemaining(kv -> {
                               builder.setSymbol(kv.value.getSymbol())
                                .setBuys(kv.value.getBuys())
                                        .setSells(kv.value.getSells());

                remoteRangeResults.add(builder.build());
            });
        });
        repsonseBuilder.addAllAggregations(remoteRangeResults);
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void multiKeyQueryService(MultKeyQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final MultiKeyQuery<String, StockTransactionAggregation> multiKeyQuery = MultiKeyQuery.<String, StockTransactionAggregation>withKeys(new HashSet<>(request.getSymbolsList()))
                .keySerde(Serdes.String())
                .valueSerde(SerdeUtil.stockTransactionAggregationSerde());
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<KeyValueIterator<String, StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(multiKeyQuery)
                .withPartitions(partitionSet));
        final QueryResult<KeyValueIterator<String, StockTransactionAggregation>> queryResult = keyQueryResult.getOnlyPartitionResult();

        KeyValueIterator<String, StockTransactionAggregation> aggregations = queryResult.getResult();
        List<StockTransactionAggregationProto> aggregationProtos = new ArrayList<>();
        StockTransactionAggregationProto.Builder builder = StockTransactionAggregationProto.newBuilder();
        aggregations.forEachRemaining(kv -> {
             StockTransactionAggregation aggregation = kv.value;
            aggregationProtos.add(builder.setSymbol(aggregation.getSymbol())
                    .setBuys(aggregation.getBuys())
                    .setSells(aggregation.getSells()).build());
        });
        responseObserver.onNext(QueryResponseProto.newBuilder().addAllAggregations(aggregationProtos).build());
        responseObserver.onCompleted();
    }
}
