package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.controller.StockController;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.query.QueryResponse;
import io.confluent.developer.streams.KafkaStreamsService;
import io.confluent.developer.streams.SerdeUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@Testcontainers
@DirtiesContext
class InteractiveQueriesIntegrationTest {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Time time = Time.SYSTEM;
    private static final int PARTITIONS = 2;
    private final NewTopic inputTopic = new NewTopic("input", PARTITIONS, (short) 1);
    private final NewTopic outputTopic = new NewTopic("output", PARTITIONS, (short) 1);
    private final List<NewTopic> topics = List.of(inputTopic, outputTopic);

    private static final int APP_ONE_PORT = 7084;
    private static final int APP_TWO_PORT = 7089;
    private static final int APP_ONE_GRPC_PORT = 5084;

    private static final int APP_TWO_GRPC_PORT = 5089;

    private static final String SYMBOL_ONE = "CFLT";
    private static final String SYMBOL_TWO = "ZELK";

    private final Random random = new Random();
    private final ObjectMapper mapper = new ObjectMapper();
    int portToSet;

    @Container
    //KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.1.arm64"));
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));


    @BeforeEach
    public void setUp() {
        final Map<String, Object> adminConfigs = Map.of("bootstrap.servers", kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminConfigs)) {
            adminClient.createTopics(topics);
        }
    }

    @AfterEach
    public void tearDown() {
        final Map<String, Object> adminConfigs = Map.of("bootstrap.servers", kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminConfigs)) {
            adminClient.deleteTopics(List.of("input", "output"));
        }
    }

    @Test
    @DisplayName("Key Query and failover with Standby Tasks")
    void testStandbyKeyQueryIQ() {

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT,APP_ONE_GRPC_PORT,"App 1 profile", kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, APP_TWO_GRPC_PORT, "App 2 profile", kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(3, SYMBOL_ONE, SYMBOL_TWO);


        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {

            QueryResponse<Map> appOneResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_ONE);
            assertThat(appOneResult.getResult().get("symbol"), is(SYMBOL_ONE));
            assertThat(appOneResult.getHostInformation(), containsString("ACTIVE"));

            QueryResponse<Map> appTwoResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_TWO);
            assertThat(appTwoResult.getResult().get("symbol"), is(SYMBOL_TWO));
            assertThat(appTwoResult.getHostInformation(), containsString("ACTIVE"));

            String appOneHostCheck = appOneResult.getHostInformation().toLowerCase().contains("grcp") ? "STANDBY" : "ACTIVE";
            String appTwoHostCheck = appTwoResult.getHostInformation().toLowerCase().contains("grcp") ? "ACTIVE" : "STANDBY";

            contextTwo.close();
            while (contextTwo.isRunning()) {
                time.sleep(500);
            }



            appOneResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_ONE);
            assertThat(appOneResult.getResult().get("symbol"), is(SYMBOL_ONE));

            assertThat(appOneResult.getHostInformation(), containsString(appOneHostCheck));
            assertThat(appOneResult.getHostInformation(), containsString(Integer.toString(APP_ONE_PORT)));

            appTwoResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_TWO);
            assertThat(appTwoResult.getResult().get("symbol"), is(SYMBOL_TWO));
            assertThat(appTwoResult.getHostInformation(), containsString(appTwoHostCheck));
            assertThat(appTwoResult.getHostInformation(), containsString(Integer.toString(APP_ONE_PORT)));

        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }
    }

    @Test
    @DisplayName("Key Query using gRPC for remote call")
    void testKeyQueryIQgRpc() {

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, APP_ONE_GRPC_PORT, "App 1 profile", kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT,APP_TWO_GRPC_PORT, "App 2 profile", kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(3, SYMBOL_ONE, SYMBOL_TWO);


        KafkaStreamsService streamsContainer = contextOne.getBean(KafkaStreamsService.class);
        KafkaStreams kafkaStreams = streamsContainer.kafkaStreams();
        Set<ThreadMetadata> metadataSet = kafkaStreams.metadataForLocalThreads();
        metadataSet.forEach((threadMetadata -> {
            threadMetadata.activeTasks().forEach((active -> {
                TopicPartition tp = active.topicPartitions().iterator().next();
                if (tp.partition() == 1) {
                    portToSet = APP_TWO_PORT;
                } else {
                    portToSet = APP_ONE_PORT;
                }
            }));
        }));

        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {
            QueryResponse<Map> appOneResult = queryForSingleResult(portToSet, "streams-iq/keyquery/" + SYMBOL_TWO);
            assertThat(appOneResult.getResult().get("symbol"), is(SYMBOL_TWO));
            assertThat(appOneResult.getHostInformation(), containsString(StockController.HostStatus.GRPC_ACTIVE.toString()));
        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }

    }


    @Test
    @DisplayName("Range Query and failover with Standby Tasks")
    void testStandbyRangeQueryIQ() {

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, APP_ONE_GRPC_PORT, "App 1 profile", kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, APP_TWO_GRPC_PORT, "App 2 profile", kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(4, SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");

        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {

            QueryResponse<List<StockTransactionAggregation>> rangeResult = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO, null);
            List<String> actualSymbols = rangeResult.getResult().stream().map(StockTransactionAggregation::getSymbol).toList();
            assertThat(actualSymbols, containsInAnyOrder(SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT"));

            contextTwo.close();
            while (contextTwo.isRunning()) {
                time.sleep(500);
            }
            QueryResponse<List<StockTransactionAggregation>> rangeResultII = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO, null);
            List<String> actualSymbolsII = rangeResultII.getResult().stream().map(StockTransactionAggregation::getSymbol).toList();
            assertThat(actualSymbolsII, containsInAnyOrder(SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT"));

        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }

    }

    @Test
    @DisplayName("Custom Range Query with filtering")
    void testFilteredRangeQueryIQ() {

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, APP_ONE_GRPC_PORT, "App 1 profile", kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, APP_TWO_GRPC_PORT, "App 2 profile", kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecordsForFilteredList(4, SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");

        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {
            QueryResponse<List<StockTransactionAggregation>> rangeResult = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO, "@.symbol == 'CFLT'");
            List<String> expectedSymbols = List.of(SYMBOL_ONE);
            assertThat(rangeResult.getResult().size(), is(1));
            assertThat(rangeResult.getResult().get(0).getSymbol(), is(expectedSymbols.get(0)));

        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }
    }

    @Test
    @DisplayName("MultiKey Query tests")
    void testMultiKeyQuery() {

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, APP_ONE_GRPC_PORT, "App 1 properties", kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, APP_TWO_GRPC_PORT, "App 2 properties", kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(3, SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");


        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {

            List<String> expectedSymbols = Arrays.asList(SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");
            String allKeys = String.join(",", expectedSymbols);
            QueryResponse<List<JsonNode>> multiKeyResults = queryForMultipleKeys(APP_ONE_PORT, "streams-iq/multikey/" + allKeys);
            assertThat(expectedSymbols, containsInAnyOrder(multiKeyResults.getResult().stream().map(jsonNode -> jsonNode.get("symbol").asText()).toArray()));

        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private QueryResponse<Map> queryForSingleResult(int port, String path) {
        QueryResponse<Map> response = restTemplate.getForObject("http://localhost:" + port + "/" + path, QueryResponse.class);
        return response;

    }

    private QueryResponse<List<JsonNode>> queryForMultipleKeys(int port, String path) {
        QueryResponse<List<LinkedHashMap<String, Object>>> response =  restTemplate.getForObject("http://localhost:" + port + "/" + path, QueryResponse.class);
        List<JsonNode> jsonNodeList = response.getResult().stream().map( lhm -> mapper.convertValue(lhm, JsonNode.class)).toList();
        return QueryResponse.withResult(jsonNodeList);
    }

    @SuppressWarnings("unchecked")
    private QueryResponse<List<StockTransactionAggregation>> queryForRangeResult(int port, String lower, String upper, String jsonFilter) {
        String url = "http://localhost:" + port + "/streams-iq/range";
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(url).queryParam("lower", lower).queryParam("upper", upper).queryParam("filter", jsonFilter);
        QueryResponse<List<LinkedHashMap<String, Object>>> response = restTemplate.getForObject(uriBuilder.build().toUri(), QueryResponse.class);
        List<StockTransactionAggregation> aggregationList = response.getResult().stream().map(lhm -> {
            String symbol = (String) lhm.get("symbol");
            double buys = (double) lhm.get("buys");
            double sells = (double) lhm.get("sells");
            return new StockTransactionAggregation(symbol, buys, sells);
        }).collect(Collectors.toList());
        return QueryResponse.withResult(aggregationList);
    }

    private ConfigurableApplicationContext createAndStartApplication(int serverPort,
                                                                     int grpcPort,
                                                                     String profile,
                                                                     String bootstrapServers) {
        SpringApplicationBuilder applicationBuilder =
                new SpringApplicationBuilder(KafkaStreamsInteractiveQueriesApp.class);
        SpringApplication kafkaStreamsApp = applicationBuilder.build();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("server.port", serverPort);
        properties.put("grpc.port", grpcPort);
        properties.put("secure.configs", "false");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 60 * 1000);
        ConfigurableEnvironment env = new StandardEnvironment();
        env.setActiveProfiles(profile);

        env.getPropertySources()
                .addFirst(new PropertiesPropertySource("initProps", properties));

        kafkaStreamsApp.setEnvironment(env);
        return kafkaStreamsApp.run();
    }

    private void produceInputRecords(int numTransactions, String... symbols) {
        Map<String, Object> producerConfigs = Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", StringSerializer.class,
                "value.serializer", SerdeUtil.stockTransactionSerde().serializer().getClass());
        try (KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            Stream.of(symbols).forEach(symbol ->
                    getTransactionList(symbol, numTransactions).forEach(txn -> producer.send(new ProducerRecord<>("input", txn.getSymbol(), txn), ((metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error producing records" + exception.getMessage());
                        } else {
                            System.out.printf("Produced record with key %s offset %d and partition %d%n", txn.getSymbol(), metadata.offset(), metadata.partition());
                        }
                    }))));
        }
    }

    private void produceInputRecordsForFilteredList(int numTransactions, String... symbols) {
        Map<String, Object> producerConfigs = Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", StringSerializer.class,
                "value.serializer", SerdeUtil.stockTransactionSerde().serializer().getClass());
        try (KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            Stream.of(symbols).forEach(symbol ->
                    getListForFilteredRange(symbol, numTransactions).forEach(txn -> producer.send(new ProducerRecord<>("input", txn.getSymbol(), txn), ((metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error producing records" + exception.getMessage());
                        } else {
                            System.out.printf("Produced record with key %s offset %d and partition %d%n", txn.getSymbol(), metadata.offset(), metadata.partition());
                        }
                    }))));
        }
    }

    private List<StockTransaction> getTransactionList(String symbol, int numTransactions) {
        var builder = StockTransaction.StockTransactionBuilder.builder();
        Stream<StockTransaction> txnStream = Stream.generate(() -> builder.withSymbol(symbol).withAmount(100.00).withBuy(random.nextBoolean()).build());
        return txnStream.limit(numTransactions).collect(Collectors.toList());
    }

    private List<StockTransaction> getListForFilteredRange(String symbol, int numTransactions) {
        var builder = StockTransaction.StockTransactionBuilder.builder();
        if (symbol.equals("CFLT")) {
            Stream<StockTransaction> buyStream = Stream.generate(() -> builder.withSymbol("CFLT").withAmount(1000).withBuy(true).build());
            Stream<StockTransaction> sellStream = Stream.generate(() -> builder.withSymbol("CFLT").withAmount(500).withBuy(false).build());
            List<StockTransaction> allTransactions = new ArrayList<>();
            allTransactions.addAll(buyStream.limit(5).toList());
            allTransactions.addAll(sellStream.limit(5).toList());
            return allTransactions;
        } else {
            return getTransactionList(symbol, numTransactions);
        }
    }


}
