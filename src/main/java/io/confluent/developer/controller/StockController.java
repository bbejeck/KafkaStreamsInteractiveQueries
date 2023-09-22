package io.confluent.developer.controller;

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
import io.confluent.developer.query.QueryResponse;
import io.confluent.developer.query.QueryUtils;
import io.confluent.developer.streams.SerdeUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.KeyQueryMetadata.NOT_AVAILABLE;

@RestController
@RequestMapping("/streams-iq")
public class StockController {
    private final KafkaStreams kafkaStreams;
    @Value("${store.name}")
    private String storeName;

    @Value("${application.server}")
    private String applicationServer;

    private HostInfo thisHostInfo;

    private ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    private static final int MAX_RETRIES = 3;
    private static final Time time = Time.SYSTEM;

    private static final boolean IS_STANDBY = true;
    private static final boolean IS_ACTIVE = false;

    public enum HostStatus {
        LOCAL_ACTIVE,
        LOCAL_STANDBY,
        GRPC_ACTIVE,
        GRPC_STANDBY
    }

    @Autowired
    public StockController(final KafkaStreams kafkaStreams,
                           final RestTemplateBuilder restTemplateBuilder) {
        this.kafkaStreams = kafkaStreams;
        this.restTemplate = restTemplateBuilder.build();
    }

    @PostConstruct
    public void init() {
        String[] parts = applicationServer.split(":");
        thisHostInfo = new HostInfo(parts[0], Integer.parseInt(parts[1]));
        objectMapper = new ObjectMapper();
    }

    @GetMapping(value = "/range")
    public QueryResponse<List<StockTransactionAggregationProto>> getAggregationRange(@RequestParam(required = false) String lower,
                                                             @RequestParam(required = false) String upper,
                                                             @RequestParam(required = false) String filter) {

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        List<StockTransactionAggregationProto> aggregations = new ArrayList<>();
        StringBuilder hostInformationStringBuilder = new StringBuilder();

        streamsMetadata.forEach(streamsClient -> {
            Set<Integer> partitions = getPartitions(streamsClient.topicPartitions());
            QueryResponse<List<StockTransactionAggregationProto>> queryResponse = doRangeQuery(streamsClient.hostInfo(),
                    Optional.of(partitions),
                    Optional.empty(),
                    lower,
                    upper,
                    filter,
                    IS_ACTIVE);
            if (!queryResponse.hasError()) {
                hostInformationStringBuilder.append(queryResponse.getHostInformation()).append(", ");
                aggregations.addAll(queryResponse.getResult());
            } else {
                List<StreamsMetadata> standbys = getStandbyClients(streamsClient, streamsMetadata);
                Optional<QueryResponse<List<StockTransactionAggregationProto>>> standbyResponse = standbys.stream().map(standby -> {
                            Set<TopicPartition> standbyTopicPartitions = getStandbyTopicPartitions(streamsClient, standby);
                            Set<Integer> standbyPartitions = getPartitions(standbyTopicPartitions);
                            return  doRangeQuery(standby.hostInfo(), Optional.of(standbyPartitions), Optional.empty(), lower, upper, filter, IS_STANDBY);
                        }).filter(Objects::nonNull)
                        .findFirst();
                standbyResponse.ifPresent(listQueryResponse -> {
                    hostInformationStringBuilder.append(listQueryResponse.getHostInformation()).append(", ");
                    aggregations.addAll(listQueryResponse.getResult());
                });
            }

        });
        return QueryResponse.withResult(aggregations).setHostInformation(hostInformationStringBuilder.toString());
    }

    private QueryResponse<List<StockTransactionAggregationProto>> doRangeQuery(final HostInfo targetHostInfo,
                                                                               final Optional<Set<Integer>> partitions,
                                                                               final Optional<PositionBound> positionBound,
                                                                               final String lower,
                                                                               final String upper,
                                                                               final String predicate,
                                                                               final boolean isStandBy) {

        Query<KeyValueIterator<String,StockTransactionAggregationProto>> rangeQuery = QueryUtils.createRangeQuery(lower, upper, predicate);

        StateQueryRequest<KeyValueIterator<String, StockTransactionAggregationProto>> queryRequest = StateQueryRequest.inStore(storeName).withQuery(rangeQuery);
        if (partitions.isPresent() && !partitions.get().isEmpty()) {
            queryRequest = queryRequest.withPartitions(partitions.get());
        }
        if (positionBound.isPresent()) {
            queryRequest = queryRequest.withPositionBound(positionBound.get());
        }

        QueryResponse<List<StockTransactionAggregationProto>> queryResponse;
        List<StockTransactionAggregationProto> aggregations = new ArrayList<>();
        if (targetHostInfo.equals(thisHostInfo)) {
            StateQueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> result = kafkaStreams.query(queryRequest);
            aggregations.addAll(extractStateQueryResults(result));
            queryResponse = QueryResponse.withResult(aggregations);
            HostStatus hostStatus = isStandBy ? HostStatus.LOCAL_STANDBY : HostStatus.LOCAL_ACTIVE;
            String hostInfo = String.format("Status[%s] %s:%d results[%d]", hostStatus, targetHostInfo.host(), targetHostInfo.port(), aggregations.size());
            queryResponse.setHostInformation(hostInfo);
        } else {
            String host = targetHostInfo.host();
            // By convention all gRPC ports are Kafka Streams host port - 2000
            int grpcPort = targetHostInfo.port() - 2000;
            queryResponse = doRemoteRangeQuery(host, grpcPort, partitions, Optional.empty(),
                    Optional.ofNullable(lower),
                    Optional.ofNullable(upper),
                    Optional.ofNullable(predicate));

            HostStatus hostStatus = isStandBy ? HostStatus.GRPC_STANDBY : HostStatus.GRPC_ACTIVE;
            if (!queryResponse.hasError()) {
                String hostInfo = String.format("Status[%s] %s:%d results[%d]", hostStatus, targetHostInfo.host(), grpcPort, queryResponse.getResult().size());
                queryResponse.setHostInformation(hostInfo);
            }
        }
        return queryResponse;
    }

    @GetMapping(value = "/keyquery/{symbol}")
    public QueryResponse<StockTransactionAggregationProto> getAggregationKeyQuery(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        HostInfo activeHost = keyMetadata.activeHost();
        Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
        KeyQuery<String, StockTransactionAggregationProto> keyQuery = KeyQuery.withKey(symbol);
        QueryResponse<StockTransactionAggregationProto> queryResponse = doKeyQuery(activeHost, keyQuery, keyMetadata, symbol, HostStatus.LOCAL_ACTIVE);
        if (queryResponse.hasError() && !standbyHosts.isEmpty()) {
            Optional<QueryResponse<StockTransactionAggregationProto>> standbyResponse = standbyHosts.stream()
                    .map(standbyHost -> doKeyQuery(standbyHost, keyQuery, keyMetadata, symbol, HostStatus.LOCAL_STANDBY))
                    .filter(resp -> resp != null && !resp.hasError())
                    .findFirst();
            if (standbyResponse.isPresent()) {
                queryResponse = standbyResponse.get();
            }
        }
        return queryResponse;
    }

    @GetMapping(value = "/multikey/{symbols}")
    public QueryResponse<List<StockTransactionAggregationProto>> getMultiAggregationKeyQuery(@PathVariable List<String> symbols) {
        Map<KeyQueryMetadata, Set<String>> metadataSymbolMap = new HashMap<>();
        symbols.forEach(symbol -> {
            KeyQueryMetadata metadata = getKeyMetadata(symbol, Serdes.String().serializer());
            if (metadata != null) {
                metadataSymbolMap.computeIfAbsent(metadata, k -> new HashSet<>()).add(symbol);
            }
        });
        return doMultiKeyQuery(metadataSymbolMap);
    }


    private QueryResponse<List<StockTransactionAggregationProto>> doMultiKeyQuery(final Map<KeyQueryMetadata,
            Set<String>> metadataSymbolMap) {

        List<StockTransactionAggregationProto> intermediateResults = new ArrayList<>();
        List<String> hostList = new ArrayList<>();
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransactionAggregationProto> txnSerde = SerdeUtil.stockTransactionAggregationProtoJsonSerde();
        metadataSymbolMap.forEach((keyMetadata, symbols) -> {
            HostInfo targetHostInfo = keyMetadata.activeHost();
            if (keyMetadata.activeHost().equals(thisHostInfo)) {
                Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
                MultiKeyQuery<String, StockTransactionAggregationProto> multiKeyQuery = MultiKeyQuery
                        .<String, StockTransactionAggregationProto>withKeys(symbols)
                        .keySerde(stringSerde)
                        .valueSerde(txnSerde);
                StateQueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> stateQueryResult =
                        kafkaStreams.query(StateQueryRequest.inStore(storeName)
                        .withQuery(multiKeyQuery)
                        .withPartitions(partitionSet));

                intermediateResults.addAll(extractStateQueryResults(stateQueryResult));

            } else {
                QueryResponse<List<StockTransactionAggregationProto>> queryResponse;
                String host = targetHostInfo.host();
                // By convention all gRPC ports are Kafka Streams host port - 2000
                int port = targetHostInfo.port() - 2000;
                queryResponse = doRemoteMultiKeyQuery(host, port, keyMetadata.partition(), symbols);
                hostList.add(queryResponse.getHostInformation());
                intermediateResults.addAll(queryResponse.getResult());
            }
        });
        QueryResponse<List<StockTransactionAggregationProto>> finalResult = QueryResponse.withResult(intermediateResults);
        finalResult.setHostInformation(hostList.toString());
        return finalResult;
    }


    private QueryResponse<StockTransactionAggregationProto> doKeyQuery(final HostInfo targetHostInfo,
                                               final Query<StockTransactionAggregationProto> query,
                                               final KeyQueryMetadata keyMetadata,
                                               final String symbol,
                                               final HostStatus hostStatus) {
        QueryResponse<StockTransactionAggregationProto> queryResponse;
        if (targetHostInfo.equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<StockTransactionAggregationProto> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(query)
                    .withPartitions(partitionSet));
            QueryResult<StockTransactionAggregationProto> queryResult = keyQueryResult.getOnlyPartitionResult();
            queryResponse = QueryResponse.withResult(queryResult.getResult());
            queryResponse.setHostInformation(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
        } else {
            String host = targetHostInfo.host();
            // By convention all gRPC ports are Kafka Streams host port - 2000
            int port = targetHostInfo.port() - 2000;
            queryResponse = doRemoteKeyQuery(host, port, keyMetadata.partition(), symbol);
        }
        return queryResponse;
    }

    private <V> QueryResponse<V> doRemoteRangeQuery(final String host,
                                                    final int port,
                                                    final Optional<Set<Integer>> partitions,
                                                    final Optional<PositionBound> positionBound,
                                                    final Optional<String> lowerBound,
                                                    final Optional<String> upperBound,
                                                    final Optional<String> predicate) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            InternalQueryGrpc.InternalQueryBlockingStub blockingStub = InternalQueryGrpc.newBlockingStub(channel);
            RangeQueryRequestProto.Builder rangeRequestBuilder = RangeQueryRequestProto.newBuilder()
                    .setUpper(upperBound.orElse(""))
                    .setLower(lowerBound.orElse(""))
                    .setPredicate(predicate.orElse(""))
                    .addAllPartitions(partitions.orElseGet(Collections::emptySet));

            QueryResponseProto rangeQueryResponse = blockingStub.rangeQueryService(rangeRequestBuilder.build());
            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(rangeQueryResponse.getAggregationsList());
        } catch (StatusRuntimeException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }

        }
        return remoteResponse;
    }

    private <V> QueryResponse<V> doRemoteKeyQuery(final String host,
                                                  final int port,
                                                  final int partition,
                                                  final String symbol) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            InternalQueryGrpc.InternalQueryBlockingStub blockingStub = InternalQueryGrpc.newBlockingStub(channel);
            KeyQueryMetadataProto keyQueryMetadata = KeyQueryMetadataProto.newBuilder().setPartition(partition).build();

            KeyQueryRequestProto keyQueryRequest = KeyQueryRequestProto.newBuilder().setSymbol(symbol).setKeyQueryMetadata(keyQueryMetadata).build();
            QueryResponseProto queryResponseProto = blockingStub.keyQueryService(keyQueryRequest);
            StockTransactionAggregationProto aggregationProto = queryResponseProto.getAggregations(0);
            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(aggregationProto);
            remoteResponse.setHostInformation(HostStatus.GRPC_ACTIVE + "-" + host + ":" + port);
        } catch (StatusRuntimeException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
        return remoteResponse;
    }

    private <V> QueryResponse<V> doRemoteMultiKeyQuery(final String host,
                                                       final int port,
                                                       final int partition,
                                                       final Set<String> symbols) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            InternalQueryGrpc.InternalQueryBlockingStub blockingStub = InternalQueryGrpc.newBlockingStub(channel);
            KeyQueryMetadataProto keyQueryMetadata = KeyQueryMetadataProto.newBuilder().setPartition(partition).build();

            MultKeyQueryRequestProto multipleRequest = MultKeyQueryRequestProto.newBuilder().addAllSymbols(symbols).setKeyQueryMetadata(keyQueryMetadata).build();
            QueryResponseProto multiKeyResponseProto = blockingStub.multiKeyQueryService(multipleRequest);
            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(multiKeyResponseProto.getAggregationsList());
            remoteResponse.setHostInformation(HostStatus.GRPC_ACTIVE + "-" + host + ":" + port);
        } catch (StatusRuntimeException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
        return remoteResponse;
    }

    private <K> KeyQueryMetadata getKeyMetadata(K key, Serializer<K> keySerializer) {
        int currentRetries = 0;
        KeyQueryMetadata keyMetadata = kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
        while (keyMetadata.equals(NOT_AVAILABLE)) {
            if (++currentRetries > MAX_RETRIES) {
                return null;
            }
            time.sleep(2000);
            keyMetadata = kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
        }
        return keyMetadata;
    }


    private Set<Integer> getPartitions(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toSet());
    }

    private List<StreamsMetadata> getStandbyClients(final StreamsMetadata currentClient, Collection<StreamsMetadata> candidates) {
        return candidates.stream().filter(streamClient -> !streamClient.equals(currentClient) &&
                streamClient.standbyStateStoreNames().contains(storeName) &&
                !getStandbyTopicPartitions(currentClient, streamClient).isEmpty()).toList();
    }

    private Set<TopicPartition> getStandbyTopicPartitions(StreamsMetadata currentClient, StreamsMetadata standbyCandidate) {
        Set<TopicPartition> temp = new HashSet<>(currentClient.topicPartitions());
        temp.retainAll(standbyCandidate.standbyTopicPartitions());
        return temp;
    }
    private List<StockTransactionAggregationProto> extractStateQueryResults(StateQueryResult<KeyValueIterator<String, StockTransactionAggregationProto>> result) {
        Map<Integer, QueryResult<KeyValueIterator<String, StockTransactionAggregationProto>>> allPartitionsResult = result.getPartitionResults();
        List<StockTransactionAggregationProto> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach((key, queryResult) -> {
            queryResult.getResult().forEachRemaining(kv -> aggregationResult.add(kv.value));
        });
        return aggregationResult;
    }
}
