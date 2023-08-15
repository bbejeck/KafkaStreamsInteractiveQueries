package io.confluent.developer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.proto.InternalQueryGrpc;
import io.confluent.developer.proto.KeyQueryMetadataProto;
import io.confluent.developer.proto.KeyQueryRequestProto;
import io.confluent.developer.proto.MultKeyQueryRequestProto;
import io.confluent.developer.proto.QueryResponseProto;
import io.confluent.developer.query.FilteredRangeQuery;
import io.confluent.developer.query.MultiKeyQuery;
import io.confluent.developer.query.QueryResponse;
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
        objectMapper = new ObjectMapper();
    }

    @GetMapping(value = "/range")
    public QueryResponse<List<JsonNode>> getAggregationRange(@RequestParam(required = false) String lower,
                                                             @RequestParam(required = false) String upper,
                                                             @RequestParam(required = false) String filter) {

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        List<JsonNode> aggregations = new ArrayList<>();

        streamsMetadata.forEach(streamsClient -> {
            Set<Integer> partitions = getPartitions(streamsClient.topicPartitions());
            QueryResponse<List<JsonNode>> queryResponse = doRangeQuery(streamsClient.hostInfo(),
                    Optional.of(partitions),
                    Optional.empty(),
                    lower,
                    upper,
                    filter);
            if (!queryResponse.hasError()) {
                aggregations.addAll(queryResponse.getResult());
            } else {
                List<StreamsMetadata> standbys = getStandbyClients(streamsClient, streamsMetadata);
                Optional<QueryResponse<List<JsonNode>>> standbyResponse = standbys.stream().map(standby -> {
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
    public QueryResponse<List<JsonNode>> getAggregationRangeInternal(@RequestParam(required = false) String lower,
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

    private QueryResponse<List<JsonNode>> doRangeQuery(final HostInfo targetHostInfo,
                                                       final Optional<Set<Integer>> partitions,
                                                       final Optional<PositionBound> positionBound,
                                                       final String lower,
                                                       final String upper,
                                                       final String filterJson) {

        Query<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> rangeQuery = createRangeQuery(lower, upper, filterJson);

        StateQueryRequest<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> queryRequest = StateQueryRequest.inStore(storeName).withQuery(rangeQuery);
        if (partitions.isPresent() && !partitions.get().isEmpty()) {
            queryRequest = queryRequest.withPartitions(partitions.get());
        }
        if (positionBound.isPresent()) {
            queryRequest = queryRequest.withPositionBound(positionBound.get());
        }

        QueryResponse<List<JsonNode>> queryResponse;
        List<JsonNode> aggregations = new ArrayList<>();
        if (targetHostInfo.equals(thisHostInfo)) {
            StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> result = kafkaStreams.query(queryRequest);
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
    public QueryResponse<JsonNode> getAggregationKeyQuery(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        HostInfo activeHost = keyMetadata.activeHost();
        Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
        KeyQuery<String, ValueAndTimestamp<JsonNode>> keyQuery = KeyQuery.withKey(symbol);
        QueryResponse<JsonNode> queryResponse = doKeyQuery(activeHost, keyQuery, keyMetadata, symbol, HostStatus.ACTIVE);
        if (queryResponse.hasError() && !standbyHosts.isEmpty()) {
            Optional<QueryResponse<JsonNode>> standbyResponse = standbyHosts.stream()
                    .map(standbyHost -> doKeyQuery(standbyHost, keyQuery, keyMetadata, symbol, HostStatus.STANDBY))
                    .filter(resp -> resp != null && !resp.hasError())
                    .findFirst();
            if (standbyResponse.isPresent()) {
                queryResponse = standbyResponse.get();
            }
        }
        return queryResponse;
    }

    @GetMapping(value = "/multikey/{symbols}")
    /*
      Organize by partition -> KeyQueryMetadata -> list symbols
     */
    public QueryResponse<List<String>> getMultiAggregationKeyQuery(@PathVariable List<String> symbols) {
        Map<KeyQueryMetadata, Set<String>> metadataSymbolMap = new HashMap<>();
        symbols.forEach(symbol -> {
            KeyQueryMetadata metadata = getKeyMetadata(symbol, Serdes.String().serializer());
            if (metadata != null) {
                metadataSymbolMap.computeIfAbsent(metadata, k -> new HashSet<>()).add(symbol);
            }
        });

        List<String> querySymbols = new ArrayList<>();
        querySymbols.add("Received multiple params - formatted as a list");
        querySymbols.addAll(symbols);
        return QueryResponse.withResult(querySymbols);
    }


    private QueryResponse<List<JsonNode>> doMultiKeyQuery(final Map<KeyQueryMetadata, Set<String>> metadataSymbolMap) {

        List<JsonNode> intermediateResults = new ArrayList<>();
        Serde<String> stringSerde = Serdes.String();
        Serde<ValueAndTimestamp<JsonNode>> txnSerde = SerdeUtil.valueAndTimestampSerde();
        metadataSymbolMap.forEach((keyMetadata, symbols) -> {
            HostInfo targetHostInfo = keyMetadata.activeHost();
            if (keyMetadata.activeHost().equals(thisHostInfo)) {
                Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
                MultiKeyQuery<String, ValueAndTimestamp<JsonNode>> multiKeyQuery = MultiKeyQuery
                        .<String, ValueAndTimestamp<JsonNode>>withKeys(symbols)
                        .keySerde(stringSerde)
                        .valueSerde(txnSerde);
                StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> stateQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                        .withQuery(multiKeyQuery)
                        .withPartitions(partitionSet));
                intermediateResults.addAll(extractStateQueryResults(stateQueryResult));

            } else {
                QueryResponse<List<JsonNode>> queryResponse;
                String host = targetHostInfo.host();
                // By convention all gRPC ports are Kafka Streams host port - 2000
                int port = targetHostInfo.port() - 2000;
                queryResponse = doRemoteMultiKeyQuery(host, port, keyMetadata.partition(), symbols);
                intermediateResults.addAll(queryResponse.getResult());
            }
        });

        return QueryResponse.withResult(intermediateResults);
    }


    private QueryResponse<JsonNode> doKeyQuery(final HostInfo targetHostInfo,
                                               final Query<ValueAndTimestamp<JsonNode>> query,
                                               final KeyQueryMetadata keyMetadata,
                                               final String symbol,
                                               final HostStatus hostStatus) {
        QueryResponse<JsonNode> queryResponse;
        if (targetHostInfo.equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<ValueAndTimestamp<JsonNode>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(query)
                    .withPartitions(partitionSet));
            QueryResult<ValueAndTimestamp<JsonNode>> queryResult = keyQueryResult.getOnlyPartitionResult();
            queryResponse = QueryResponse.withResult(objectMapper.valueToTree(queryResult.getResult().value()));
            queryResponse.setHostType(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
        } else {
            String host = targetHostInfo.host();
            // By convention all gRPC ports are Kafka Streams host port - 2000
            int port = targetHostInfo.port() - 2000;
            queryResponse = doRemoteKeyQuery(host, port, keyMetadata.partition(), symbol);
        }
        return queryResponse;
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
            QueryResponseProto queryResponseProto = blockingStub.getAggregationForSymbol(keyQueryRequest);

            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(queryResponseProto.getJsonResultsList());
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

    private <V> QueryResponse<V> doRemoteMultiKeyQuery(String host, int port, int partition, Set<String> symbols) {
        QueryResponse<V> remoteResponse;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            InternalQueryGrpc.InternalQueryBlockingStub blockingStub = InternalQueryGrpc.newBlockingStub(channel);
            KeyQueryMetadataProto keyQueryMetadata = KeyQueryMetadataProto.newBuilder().setPartition(partition).build();

            MultKeyQueryRequestProto multipleRequest = MultKeyQueryRequestProto.newBuilder().addAllSymbols(symbols).setKeyQueryMetadata(keyQueryMetadata).build();
            QueryResponseProto multiKeyResponseProto = blockingStub.getAggregationsForSymbols(multipleRequest);
            remoteResponse = (QueryResponse<V>) QueryResponse.withResult(multiKeyResponseProto.getJsonResultsList());
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

    private Query<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> createRangeQuery(String lower, String upper, String jsonPredicate) {
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

    private FilteredRangeQuery<String, ValueAndTimestamp<JsonNode>> createFilteredRangeQuery(String lower, String upper, String jsonPredicate) {
        return FilteredRangeQuery.<String, ValueAndTimestamp<JsonNode>>withPredicate(jsonPredicate).serdes(Serdes.String(), SerdeUtil.valueAndTimestampSerde());
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

    private List<JsonNode> extractStateQueryResults(StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>> result) {
        Map<Integer, QueryResult<KeyValueIterator<String, ValueAndTimestamp<JsonNode>>>> allPartitionsResult = result.getPartitionResults();
        List<JsonNode> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach((key, queryResult) -> {
            queryResult.getResult().forEachRemaining(kv -> aggregationResult.add(kv.value.value()));
        });
        return aggregationResult;
    }
}
