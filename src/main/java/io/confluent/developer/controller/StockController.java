package io.confluent.developer.controller;

import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.query.MultiKeyQuery;
import io.confluent.developer.query.QueryResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
    private final Time time = Time.SYSTEM;

    private static final String IQ_URL = "http://{host}:{port}/streams-iq{path}";

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

    @GetMapping(value = "/v2/range")
    public QueryResponse<List<StockTransactionAggregation>> getAggregationRangeV2(@RequestParam(required = false) String lower,
                                                                                  @RequestParam(required = false) String upper) {

        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        List<StockTransactionAggregation> aggregations = new ArrayList<>();
        String path = String.format("/range?lower=%s&upper=%s", lower, upper);

        RangeQuery<String, ValueAndTimestamp<StockTransactionAggregation>> rangeQuery = createRangeQuery(lower, upper);
        streamsMetadata.forEach(metadata -> {
            if (metadata.hostInfo().equals(thisHostInfo)) {
                StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>>> result =
                        kafkaStreams.query(StateQueryRequest.inStore(storeName).withQuery(rangeQuery));
                aggregations.addAll(extractStateQueryResults(result));
            } else {
                String host = metadata.host();
                int port = metadata.port();
                QueryResponse<List<StockTransactionAggregation>> otherResponse = restTemplate.getForObject(IQ_URL, QueryResponse.class, host, port, path);
                if (otherResponse != null) {
                    aggregations.addAll(otherResponse.getResult());
                }
            }
        });

        return QueryResponse.withResult(aggregations);
    }

    @GetMapping(value = "/v1/get/{symbol}")
    public QueryResponse<StockTransactionAggregation> getAggregation(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }

        if (keyMetadata.activeHost().equals(thisHostInfo)) {
            StoreQueryParameters<ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>>> storeParams =
                    StoreQueryParameters.<ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>>>
                                    fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStore())
                            .withPartition(keyMetadata.partition())
                            .enableStaleStores();

            ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>> readOnlyAggStore = kafkaStreams.store(storeParams);
            ValueAndTimestamp<StockTransactionAggregation> valueAndTimestamp = readOnlyAggStore.get(symbol);
            
            QueryResponse<StockTransactionAggregation> queryResponse;
            if (valueAndTimestamp != null) {
                queryResponse = QueryResponse.withResult(valueAndTimestamp.value());
            } else {
                queryResponse = QueryResponse.withError(String.format("Key [%s] not found", symbol));
            }
            return queryResponse;
        } else {
            String host = keyMetadata.activeHost().host();
            int port = keyMetadata.activeHost().port();
            String path = String.format("/v1/get/%s", symbol);

            QueryResponse<StockTransactionAggregation> remoteResponse;
            try {
                remoteResponse = restTemplate.getForObject(IQ_URL, QueryResponse.class, host, port, path);
            } catch (RestClientException exception) {
                remoteResponse = QueryResponse.withError(exception.getMessage());
            }
            return remoteResponse;
        }
    }

    @GetMapping(value = "/v1/range")
    public QueryResponse<List<StockTransactionAggregation>> getAggregationRangeV1(@RequestParam(required = false) String lower,
                                                                                  @RequestParam(required = false) String upper) {
        Collection<StreamsMetadata> streamsMetadata = kafkaStreams.streamsMetadataForStore(storeName);
        if (streamsMetadata.isEmpty()) {
            return QueryResponse.withError(String.format("ERROR: No metadata for store [%s], retry or confirm store name", storeName));
        }
        String lowerParam = lower != null && lower.isEmpty() ? null : lower;
        String upperParam = upper != null && upper.isEmpty() ? null : upper;
        String path = String.format("/v1/range/?lower=%s&upper=%s", lower, upper);
        List<StockTransactionAggregation> aggregations = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (metadata.hostInfo().equals(thisHostInfo)) {
                StoreQueryParameters<ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>>> storeParams =
                        StoreQueryParameters.<ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>>>
                                        fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStore())
                                .enableStaleStores();
                ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockTransactionAggregation>> readOnlyAggStore = kafkaStreams.store(storeParams);
                try (KeyValueIterator<String, ValueAndTimestamp<StockTransactionAggregation>> iterator = readOnlyAggStore.range(lowerParam, upperParam)) {
                    while (iterator.hasNext()) {
                        aggregations.add(iterator.next().value.value());
                    }
                }
            } else {
                String host = metadata.host();
                int port = metadata.port();
                QueryResponse<List<StockTransactionAggregation>> otherResponse;
                try {
                    otherResponse = restTemplate.getForObject(IQ_URL, QueryResponse.class, host, port, path);
                } catch (RestClientException exception) {
                    otherResponse = QueryResponse.withError(String.format("Exception [%s] querying host [%s]", exception.getMessage(), host));
                }
                aggregations.addAll(otherResponse.getResult());
            }
        });

        return QueryResponse.withResult(aggregations);
    }

    @GetMapping(value = "/v2/keyquery/{symbol}")
    public QueryResponse<StockTransactionAggregation> getAggregationKeyQuery(@PathVariable String symbol) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        KeyQuery<String, ValueAndTimestamp<StockTransactionAggregation>> keyQuery = KeyQuery.withKey(symbol);
        if (keyMetadata.activeHost().equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<ValueAndTimestamp<StockTransactionAggregation>> keyQueryResult = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(keyQuery)
                    .withPartitions(partitionSet));
            QueryResult<ValueAndTimestamp<StockTransactionAggregation>> queryResult = keyQueryResult.getOnlyPartitionResult();
            return QueryResponse.withResult(queryResult.getResult().value());
        } else {
            String path = String.format("/v2/keyquery/%s", symbol);
            String host = keyMetadata.activeHost().host();
            int port = keyMetadata.activeHost().port();
            return restTemplate.getForObject(IQ_URL, QueryResponse.class, host, port, path);
        }
    }

    @GetMapping(value = "/v2/multikey/{symbols}")
    public QueryResponse<Map<String, Long>> getAggregationsMultiKey(@PathVariable String[] symbols) {
        Map<HostInfo, Set<String>> hostInfoWithKeys = new HashMap<>();
        Map<HostInfo, Set<HostInfo>> activeToStandby = new HashMap<>();
        Map<String, Long> resultMap = new HashMap<>();

        for (String symbol : symbols) {
            KeyQueryMetadata keyMetadata = getKeyMetadata(symbol, Serdes.String().serializer());
            if (keyMetadata == null) {
                return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
            }
            hostInfoWithKeys.computeIfAbsent(keyMetadata.activeHost(), k -> new HashSet<>()).add(symbol);
            Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
            activeToStandby.computeIfAbsent(keyMetadata.activeHost(), k -> standbyHosts).addAll(standbyHosts);
        }
        Map<String, Set<String>> perHostExecutionInfo = new HashMap<>();
        Set<String> localKeys = hostInfoWithKeys.remove(this.thisHostInfo);
        if (localKeys != null) {
            MultiKeyQuery<String, Long> multiKeyQuery = MultiKeyQuery.<String, Long>withKeys(localKeys).keySerde(Serdes.String()).valueSerde(Serdes.Long());

            System.out.printf("Issuing a Multi key query for the local instance %n");
            StateQueryResult<KeyValueIterator<String, Long>> result = kafkaStreams.query(StateQueryRequest.inStore(storeName)
                    .withQuery(multiKeyQuery)
                    .enableExecutionInfo());
            List<KeyValueIterator<String, Long>> iqResults = new ArrayList<>();
            Map<Integer, QueryResult<KeyValueIterator<String, Long>>> allPartitionsResult = result.getPartitionResults();
            allPartitionsResult.forEach((key, queryResult) -> {
                List<String> filtered = queryResult.getExecutionInfo().stream().filter(info -> info.contains("MultiKeyQuery")).collect(Collectors.toList());
                perHostExecutionInfo.computeIfAbsent(thisHostInfo.host() + "->partition_" + key, (k -> new LinkedHashSet<>())).addAll(filtered);
                iqResults.add(queryResult.getResult());
            });
            iqResults.forEach(iter -> iter.forEachRemaining(kv -> {
                System.out.printf("Query result key[%s] value[%s]%n", kv.key, kv.value);
                resultMap.put(kv.key, kv.value);
            }));

        }

        if (!hostInfoWithKeys.isEmpty()) {
            hostInfoWithKeys.forEach((activeHostInfo, keyList) -> {
                List<HostInfo> activeWithStandby = new ArrayList<>();
                activeWithStandby.add(activeHostInfo);
                activeWithStandby.addAll(activeToStandby.get(activeHostInfo));
                Map<String, Long> otherResults = new HashMap<>();
                RestClientException restClientException = null;
                for (HostInfo hostInfo : activeWithStandby) {
                    try {
                        otherResults = restTemplate.getForObject(IQ_URL, Map.class, hostInfo.host(), hostInfo.port(), String.join(",", keyList));
                        if (!otherResults.isEmpty()) {
                            resultMap.putAll(otherResults);
                            break;
                        }
                    } catch (RestClientException e) {
                        restClientException = e;
                    }
                }
                if (restClientException != null && resultMap.isEmpty() && otherResults.isEmpty()) {
                    throw new IllegalStateException("Could not reach either Active or Standby host for other Kafka Streams instance", restClientException);
                }
            });
        }
        return QueryResponse.withResult(resultMap).addExecutionInfo(perHostExecutionInfo);

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
