package io.confluent.developer.controller;

import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.InternalQueryGrpc;
import io.confluent.developer.proto.KeyQueryRequest;
import io.confluent.developer.proto.StockTransactionAggregationResponse;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.QueryResponse;
import io.confluent.developer.streams.SerdeUtil;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

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
import java.util.function.BiPredicate;
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
    private final RestTemplate restTemplate;

    private static final int MAX_RETRIES = 3;
    private static final Time time = Time.SYSTEM;

    private static final String BASE_IQ_URL = "http://{host}:{port}/streams-iq";

    private enum HostStatus {
        ACTIVE,
        STANDBY,
        ACTIVE_GRPC
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
    }

    @GetMapping(value = "/range")
    public QueryResponse<List<StockTransactionAggregation>> getAggregationRange(@RequestParam(required = false) String lower,
                                                                                @RequestParam(required = false) String upper,
                                                                                @RequestParam(required = false) String filter) {

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        List<StockTransactionAggregation> aggregations = new ArrayList<>();

        streamsMetadata.forEach(streamsClient -> {
            Set<Integer> partitions = getPartitions(streamsClient.topicPartitions());
            QueryResponse<List<StockTransactionAggregation>> queryResponse = doRangeQuery(streamsClient.hostInfo(),
                    Optional.of(partitions),
                    Optional.empty(),
                    lower,
                    upper,
                   filter);
            if (!queryResponse.hasError()) {
                aggregations.addAll(queryResponse.getResult());
            } else {
                List<StreamsMetadata> standbys = getStandbyClients(streamsClient, streamsMetadata);
                Optional<QueryResponse<List<StockTransactionAggregation>>> standbyResponse = standbys.stream().map(standby -> {
                            Set<TopicPartition> standbyTopicPartitions = getStandbyTopicPartitions(streamsClient, standby);
                            Set<Integer> standbyPartitions = getPartitions(standbyTopicPartitions);
                            return doRangeQuery(standby.hostInfo(), Optional.of(standbyPartitions), Optional.empty(), lower, upper, filter);
                        }).filter(Objects::nonNull)
                        .findFirst();
                standbyResponse.ifPresent(listQueryResponse -> aggregations.addAll(listQueryResponse.getResult()));
            }

        });
        return QueryResponse.withResult(aggregations);
    }


    @GetMapping(value = "/internal/range")
    public QueryResponse<List<StockTransactionAggregation>> getAggregationRangeInternal(@RequestParam(required = false) String lower,
                                                                                        @RequestParam(required = false) String upper,
                                                                                        @RequestParam(required = false) List<Integer> partitions,
                                                                                        @RequestParam(required = false) String filterJson) {
        Optional<Set<Integer>> optionalPartitions;
        if (partitions != null && !partitions.isEmpty()) {
            optionalPartitions = Optional.of(new HashSet<>(partitions));
        } else {
            optionalPartitions = Optional.empty();
        }

        return doRangeQuery(thisHostInfo, optionalPartitions, Optional.empty(), lower, upper, filterJson);

    }

    private QueryResponse<List<StockTransactionAggregation>> doRangeQuery(final HostInfo targetHostInfo,
                                                                          final Optional<Set<Integer>> partitions,
                                                                          final Optional<PositionBound> positionBound,
                                                                          final String lower,
                                                                          final String upper,
                                                                          final String filterJson) {
        
        Query<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> rangeQuery = createRangeQuery(lower, upper, filterJson);
        
        StateQueryRequest<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> queryRequest = StateQueryRequest.inStore(storeName).withQuery(rangeQuery);
        if (partitions.isPresent() && !partitions.get().isEmpty()) {
            queryRequest = queryRequest.withPartitions(partitions.get());
        }
        if (positionBound.isPresent()) {
            queryRequest = queryRequest.withPositionBound(positionBound.get());
        }

        QueryResponse<List<StockTransactionAggregation>> queryResponse;
        List<StockTransactionAggregation> aggregations = new ArrayList<>();
        if (targetHostInfo.equals(thisHostInfo)) {
            StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> result = kafkaStreams.query(queryRequest);
            aggregations.addAll(extractStateQueryResults(result));
            queryResponse = QueryResponse.withResult(aggregations);
        } else {
            String path = createRangeRequestPath(lower, upper, filterJson, partitions);
            String host = targetHostInfo.host();
            int port = targetHostInfo.port();
            queryResponse = doRemoteRequest(host, port, path);
        }
        return queryResponse;
    }

    @GetMapping(value = "/keyquery/{symbol}")
    public QueryResponse<StockTransactionAggregationResponse> getAggregationKeyQuery(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        HostInfo activeHost = keyMetadata.activeHost();
        Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
        KeyQuery<String, ValueAndTimestamp<StockTransactionAggregation>> keyQuery = KeyQuery.withKey(symbol);
        QueryResponse<StockTransactionAggregationResponse> queryResponse = doKeyQuery(activeHost, keyQuery, keyMetadata, symbol, HostStatus.ACTIVE);
        if (queryResponse.hasError() && !standbyHosts.isEmpty()) {
            Optional<QueryResponse<StockTransactionAggregationResponse>> standbyResponse = standbyHosts.stream()
                    .map(standbyHost -> doKeyQuery(standbyHost, keyQuery, keyMetadata, symbol, HostStatus.STANDBY))
                    .filter(resp -> resp != null && !resp.hasError())
                    .findFirst();
            if (standbyResponse.isPresent()) {
                queryResponse = standbyResponse.get();
            }
        }
        return queryResponse;
    }

    @GetMapping(value="/multikey/{symbols}")
    /*
      Organize by partition -> KeyQueryMetadata -> list symbols
     */
    public QueryResponse<List<String>> getMultiAggregationKeyQuery(@PathVariable List<String> symbols) {
        Map<KeyQueryMetadata, List<String>> metadataSymbolMap =  new HashMap<>();
        symbols.forEach(symbol -> {
            KeyQueryMetadata metadata = getKeyMetadata(symbol, Serdes.String().serializer());
            if (metadata != null) {
                metadataSymbolMap.computeIfAbsent(metadata, k -> new ArrayList<>()).add(symbol);
            }
        });

        List<String> querySymbols = new ArrayList<>();
        querySymbols.add("Received multiple params - formatted as a list");
        querySymbols.addAll(symbols);
        return QueryResponse.withResult(querySymbols);
    }


    private QueryResponse<List<StockTransactionAggregationResponse>> doMultiKeyQuery(final Map<KeyQueryMetadata, List<String>> metadataSymbolMap,
                                                                          final Query<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> query,
                                                                          final HostStatus hostStatus) {
        QueryResponse<List<StockTransactionAggregationResponse>> queryResponse;
        metadataSymbolMap.forEach((keyMetadata, symbols) -> {
            if (keyMetadata.activeHost().equals(thisHostInfo)) {
                Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
                StateQueryResult<KeyValueIterator<String,ValueAndTimestamp<StockTransactionAggregation>>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                        .withQuery(query)
                        .withPartitions(partitionSet));
                QueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> queryResult = keyQueryResult.getOnlyPartitionResult();
                StockTransactionAggregationResponse stockTransactionAggregationResponse = StockTransactionAggregationResponse.newBuilder()
                        .setBuys(queryResult.getResult().value().getBuys())
                        .setSells(queryResult.getResult().value().getSells())
                        .setSymbol(queryResult.getResult().value().getSymbol()).build();
                queryResponse = QueryResponse.withResult(stockTransactionAggregationResponse);
                queryResponse.setHostType(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
            } else {
                String host = targetHostInfo.host();
                // By convention all gRPC ports are Kafka Streams host port - 2000
                int port = targetHostInfo.port() - 2000;
                queryResponse = doRemoteKeyRequest(host, port, keyMetadata.partition(), symbol);
            }
        });

        return queryResponse;
    }


    private QueryResponse<StockTransactionAggregationResponse> doKeyQuery(final HostInfo targetHostInfo,
                                                                  final Query<ValueAndTimestamp<StockTransactionAggregation>> query,
                                                                  final KeyQueryMetadata keyMetadata,
                                                                  final String symbol,
                                                                  final HostStatus hostStatus) {
        QueryResponse<StockTransactionAggregationResponse> queryResponse;
        if (targetHostInfo.equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<ValueAndTimestamp<StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(query)
                    .withPartitions(partitionSet));
            QueryResult<ValueAndTimestamp<StockTransactionAggregation>> queryResult = keyQueryResult.getOnlyPartitionResult();
            StockTransactionAggregationResponse stockTransactionAggregationResponse = StockTransactionAggregationResponse.newBuilder()
                    .setBuys(queryResult.getResult().value().getBuys())
                    .setSells(queryResult.getResult().value().getSells())
                    .setSymbol(queryResult.getResult().value().getSymbol()).build();
            queryResponse = QueryResponse.withResult(stockTransactionAggregationResponse);
            queryResponse.setHostType(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
        } else {
            String host = targetHostInfo.host();
            // By convention all gRPC ports are Kafka Streams host port - 2000
            int port = targetHostInfo.port() - 2000;
            queryResponse = doRemoteKeyRequest(host, port, keyMetadata.partition(), symbol);
        }
        return queryResponse;
    }


    private <V> QueryResponse<V> doRemoteKeyRequest(String host, int port, int partition, String symbol) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            InternalQueryGrpc.InternalQueryBlockingStub blockingStub = InternalQueryGrpc.newBlockingStub(channel);
            io.confluent.developer.proto.KeyQueryMetadata keyQueryMetadata = io.confluent.developer.proto.KeyQueryMetadata.newBuilder().setPartition(partition).build();
            KeyQueryRequest keyQueryRequest = KeyQueryRequest.newBuilder().setSymbol(symbol).setKeyQueryMetadata(keyQueryMetadata).build();
            StockTransactionAggregationResponse aggregationResponse = blockingStub.getAggregationForSymbol(keyQueryRequest);
            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(aggregationResponse);
            remoteResponse.setHostType(HostStatus.ACTIVE_GRPC + "-" + host + ":" + port);
        } catch (StatusRuntimeException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
        return remoteResponse;
    }

    private <V> QueryResponse<V> doRemoteRequest(String host, int port, String path) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            remoteResponse = restTemplate.getForObject(BASE_IQ_URL + path, QueryResponse.class, host, port);
            if (remoteResponse == null) {
                remoteResponse = QueryResponse.withError("Remote call returned null response");
            }
        } catch (RestClientException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
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

    private Query<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> createRangeQuery(String lower, String upper, String jsonPredicate) {
        if(isNotBlank(jsonPredicate)) {
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

    private FilteredRangeQuery<String, ValueAndTimestamp<StockTransactionAggregation>> createFilteredRangeQuery(String lower, String upper, String jsonPredicate) {
        BiPredicate<String, ValueAndTimestamp<StockTransactionAggregation>> predicate = (key, vt) ->  {
            StockTransactionAggregation agg = vt.value();
            return (agg.getBuys() >= (agg.getSells() * 2.0)) && (agg.getBuys() + agg.getSells()) > 3000.00;
        };
        return FilteredRangeQuery.withPredicate(predicate).serdes(Serdes.String(), SerdeUtil.valueAndTimestampSerde());
    }

    private String createRangeRequestPath(String lower, String upper, String filter, Optional<Set<Integer>> optionalPartitions) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("/internal")
                .queryParam("lower", lower)
                .queryParam("upper", upper)
                .queryParam("filter", filter)
                .queryParam("partitions", optionalPartitions.orElse(new HashSet<>()));
        return uriBuilder.build().getPath();
    }

    private boolean isBlank(String str) {
        return str == null || str.isBlank();
    }

    private boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    private List<StockTransactionAggregation> extractStateQueryResults(StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> result) {
        Map<Integer, QueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>>> allPartitionsResult = result.getPartitionResults();
        List<StockTransactionAggregation> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach((key, queryResult) -> {
            queryResult.getResult().forEachRemaining(kv -> aggregationResult.add(kv.value.value()));
        });
        return aggregationResult;
    }
}
