package io.confluent.developer.controller;

import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.query.QueryResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.KeyQuery;
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

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private final RestTemplate restTemplate;

    private static final int MAX_RETRIES = 3;
    private static final Time time = Time.SYSTEM;

    private static final String BASE_IQ_URL = "http://{host}:{port}/streams-iq";

    private enum HostStatus {
        ACTIVE,
        STANDBY
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
                                                                                @RequestParam(required = false) String upper) {

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        List<StockTransactionAggregation> aggregations = new ArrayList<>();
        List<StreamsMetadata> standbyHostMetadata = streamsMetadata.stream().filter(metadata -> metadata.standbyStateStoreNames()
                .contains(storeName))
                .collect(Collectors.toList());
        
        streamsMetadata.forEach(metadata -> {
            if (metadata.hostInfo().equals(thisHostInfo)) {
                QueryResponse<List<StockTransactionAggregation>> localResults = getAggregationRangeInternal(lower, upper);
                aggregations.addAll(localResults.getResult());
            } else {
                String host = metadata.host();
                int port = metadata.port();
                String path = "/internal" + createRangePath(lower, upper);
                QueryResponse<List<StockTransactionAggregation>> otherResponse = doRemoteRequest(host, port, path);
                if(! otherResponse.hasError()) {
                    aggregations.addAll(otherResponse.getResult());
                } else {
                    List<StreamsMetadata> standbyCandidates = standbyHostMetadata.stream().filter(standby -> standby.standbyTopicPartitions().equals(metadata.topicPartitions())).collect(Collectors.toList());
                    Optional<QueryResponse<List<StockTransactionAggregation>>> standbyResponse = standbyCandidates.stream()
                            .map(standbyMetadata -> {
                                QueryResponse<List<StockTransactionAggregation>> response = doRemoteRequest(standbyMetadata.host(), standbyMetadata.port(), path);
                                return response;
                            })
                            .filter(resp -> resp != null && !resp.hasError())
                            .findFirst();
                    standbyResponse.ifPresent(listQueryResponse -> aggregations.addAll(standbyResponse.get().getResult()));
                }
            }
        });
        return QueryResponse.withResult(aggregations);
    }

    @GetMapping(value = "/internal/range")
    public QueryResponse<List<StockTransactionAggregation>> getAggregationRangeInternal(@RequestParam(required = false) String lower,
                                                                                        @RequestParam(required = false) String upper) {
        RangeQuery<String, ValueAndTimestamp<StockTransactionAggregation>> rangeQuery = createRangeQuery(lower, upper);

        StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> result =
                kafkaStreams.query(StateQueryRequest.inStore(storeName).withQuery(rangeQuery));
        List<StockTransactionAggregation> aggregations = new ArrayList<>(extractStateQueryResults(result));

        return QueryResponse.withResult(aggregations);
    }

    @GetMapping(value = "/keyquery/{symbol}")
    public QueryResponse<StockTransactionAggregation> getAggregationKeyQuery(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        HostInfo activeHost = keyMetadata.activeHost();
        Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
        KeyQuery<String, ValueAndTimestamp<StockTransactionAggregation>> keyQuery = KeyQuery.withKey(symbol);
        QueryResponse<StockTransactionAggregation> queryResponse = doKeyQuery(activeHost, keyQuery, keyMetadata, symbol, HostStatus.ACTIVE);
        if (queryResponse.hasError() && !standbyHosts.isEmpty()) {
            Optional<QueryResponse<StockTransactionAggregation>> standbyResponse = standbyHosts.stream()
                    .map(standbyHost -> doKeyQuery(standbyHost, keyQuery, keyMetadata, symbol, HostStatus.STANDBY))
                    .filter(resp -> resp != null && !resp.hasError())
                    .findFirst();
            if (standbyResponse.isPresent()) {
                queryResponse = standbyResponse.get();
            }
        }
        return queryResponse;
    }


    private QueryResponse<StockTransactionAggregation> doKeyQuery(final HostInfo targetHostInfo,
                                                                  final Query<ValueAndTimestamp<StockTransactionAggregation>> query,
                                                                  final KeyQueryMetadata keyMetadata,
                                                                  final String symbol,
                                                                  final HostStatus hostStatus) {
        QueryResponse<StockTransactionAggregation> queryResponse;
        if (targetHostInfo.equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<ValueAndTimestamp<StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(query)
                    .withPartitions(partitionSet));
            QueryResult<ValueAndTimestamp<StockTransactionAggregation>> queryResult = keyQueryResult.getOnlyPartitionResult();
            queryResponse = QueryResponse.withResult(queryResult.getResult().value());
        } else {
            String path = "/keyquery/" + symbol;
            String host = targetHostInfo.host();
            int port = targetHostInfo.port();
            queryResponse = doRemoteRequest(host, port, path);
        }
        return queryResponse.setHostType(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
    }


    private <V> QueryResponse<V> doRemoteRequest(String host, int port, String path) {
        QueryResponse<V> remoteResponse;
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

    private RangeQuery<String, ValueAndTimestamp<StockTransactionAggregation>> createRangeQuery(String lower, String upper) {
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

    private String createRangePath(String lower, String upper) {
        if (isBlank(lower) && isBlank(upper)) {
            return "/range";
        } else if (!isBlank(lower) && isBlank(upper)) {
            return "/range?lower=" + lower;
        } else if (isBlank(lower) && !isBlank(upper)) {
            return "/range?upper=" + upper;
        } else {
            return "/range?lower=" + lower + "&upper=" + upper;
        }
    }

    private boolean isBlank(String str) {
        return str == null || str.isBlank();
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
