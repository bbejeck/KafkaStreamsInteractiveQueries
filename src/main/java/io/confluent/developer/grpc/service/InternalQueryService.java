package io.confluent.developer.grpc.service;

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

        final KeyQuery<String, StockTransactionAggregationProto> keyQuery = KeyQuery.withKey(request.getSymbol());
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<StockTransactionAggregationProto> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(keyQuery)
                .withPartitions(partitionSet));
        final QueryResult<StockTransactionAggregationProto> queryResult = keyQueryResult.getOnlyPartitionResult();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();
        StockTransactionAggregationProto aggregation = queryResult.getResult();
        repsonseBuilder.addAllExecutionInfo(queryResult.getExecutionInfo());
        repsonseBuilder.addAggregations(aggregation);
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rangeQueryService(RangeQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final Query<KeyValueIterator<String, StockTransactionAggregationProto>> rangeQuery =
                QueryUtils.createRangeQuery(request.getLower(), request.getUpper(), request.getPredicate());

        final StateQueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(rangeQuery)
                .withPartitions(new HashSet<>(request.getPartitionsList())));
        final Map<Integer, QueryResult<KeyValueIterator<String, StockTransactionAggregationProto>>> allPartitionResults = keyQueryResult.getPartitionResults();

        final QueryResponseProto.Builder repsonseBuilder = QueryResponseProto.newBuilder();
        allPartitionResults.forEach((k, v) -> v.getResult().forEachRemaining(kv -> repsonseBuilder.addAggregations(kv.value)));
        if (repsonseBuilder.getAggregationsBuilderList() == null) {
            repsonseBuilder.addAllAggregations(new ArrayList<>());
        }
        responseObserver.onNext(repsonseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void multiKeyQueryService(MultKeyQueryRequestProto request, StreamObserver<QueryResponseProto> responseObserver) {
        final MultiKeyQuery<String, StockTransactionAggregationProto> multiKeyQuery = MultiKeyQuery.<String, StockTransactionAggregationProto>withKeys(new HashSet<>(request.getSymbolsList()))
                .keySerde(Serdes.String())
                .valueSerde(SerdeUtil.stockTransactionAggregationProtoJsonSerde());
        final KeyQueryMetadataProto keyMetadata = request.getKeyQueryMetadata();
        final Set<Integer> partitionSet = Collections.singleton(keyMetadata.getPartition());
        final StateQueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                .withQuery(multiKeyQuery)
                .withPartitions(partitionSet));
        final QueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> queryResult = keyQueryResult.getOnlyPartitionResult();

        KeyValueIterator<String, StockTransactionAggregationProto> aggregations = queryResult.getResult();
        List<StockTransactionAggregationProto> aggregationProtos = new ArrayList<>();
        aggregations.forEachRemaining(kv -> aggregationProtos.add(kv.value));
        responseObserver.onNext(QueryResponseProto.newBuilder().addAllAggregations(aggregationProtos).build());
        responseObserver.onCompleted();
    }
}
