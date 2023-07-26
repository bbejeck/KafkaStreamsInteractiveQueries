package io.confluent.developer;

import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.query.QueryResponse;
import io.confluent.developer.streams.SerdeUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
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
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@Testcontainers
class InteractiveQueriesIntegrationTest {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Time time = Time.SYSTEM;
    private static final int PARTITIONS = 2;
    private final NewTopic inputTopic = new NewTopic("input", PARTITIONS, (short) 1);
    private final NewTopic outputTopic = new NewTopic("output", PARTITIONS, (short) 1);
    private final List<NewTopic> topics = List.of(inputTopic, outputTopic);

    private static final int APP_ONE_PORT = 7084;
    private static final int APP_TWO_PORT = 7089;

    private static final String SYMBOL_ONE = "CFLT";
    private static final String SYMBOL_TWO = "ZELK";

    private final Random random  = new Random();
    
    @Container
    //KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.1.arm64"));
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0.amd64"));


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

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(3, SYMBOL_ONE, SYMBOL_TWO);


        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {
            QueryResponse<StockTransactionAggregation> appOneResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_ONE);
            assertThat(appOneResult.getResult().getSells(), is(300.00));
            assertThat(appOneResult.getResult().getSymbol(), is(SYMBOL_ONE));
            assertThat(appOneResult.getHostType(), containsString("ACTIVE"));

            QueryResponse<StockTransactionAggregation> appTwoResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_TWO);
            assertThat(appTwoResult.getResult().getSells(), is(300.00));
            assertThat(appTwoResult.getResult().getSymbol(), is(SYMBOL_TWO));
            assertThat(appTwoResult.getHostType(), containsString("ACTIVE"));

            contextTwo.close();
            while (contextTwo.isRunning()) {
                time.sleep(500);
            }

            appOneResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_ONE);
            assertThat(appOneResult.getResult().getSells(), is(300.00));
            assertThat(appOneResult.getResult().getSymbol(), is(SYMBOL_ONE));
            assertThat(appOneResult.getHostType(), containsString("STANDBY"));
            assertThat(appOneResult.getHostType(), containsString(Integer.toString(APP_ONE_PORT)));

            appTwoResult = queryForSingleResult(APP_ONE_PORT, "streams-iq/keyquery/" + SYMBOL_TWO);
            assertThat(appTwoResult.getResult().getSells(), is(300.00));
            assertThat(appTwoResult.getResult().getSymbol(), is(SYMBOL_TWO));
            assertThat(appTwoResult.getHostType(), containsString("ACTIVE"));
            assertThat(appTwoResult.getHostType(), containsString(Integer.toString(APP_ONE_PORT)));

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

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecords(4, SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");

        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {

            QueryResponse<List<StockTransactionAggregation>> rangeResult = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO);
            List<String> expectedSymbols = List.of(SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");
            assertThat(expectedSymbols, containsInAnyOrder(rangeResult.getResult().stream().map(StockTransactionAggregation::getSymbol).toArray()));

            contextTwo.close();
            while (contextTwo.isRunning()) {
                time.sleep(500);
            }
            QueryResponse<List<StockTransactionAggregation>> rangeResultII = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO);
            assertThat(rangeResult.getResult(), containsInAnyOrder(rangeResultII.getResult().toArray()));

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

        ConfigurableApplicationContext contextOne = createAndStartApplication(APP_ONE_PORT, kafka.getBootstrapServers());
        while (!contextOne.isRunning()) {
            time.sleep(500);
        }

        time.sleep(5000);
        ConfigurableApplicationContext contextTwo = createAndStartApplication(APP_TWO_PORT, kafka.getBootstrapServers());
        while ((!contextTwo.isRunning())) {
            time.sleep(500);
        }

        time.sleep(5000);
        produceInputRecordsForFilteredList(4,SYMBOL_ONE, SYMBOL_TWO, "GOOGL", "SHMDF", "TWTR", "MSFT");

        // Time for Kafka Streams to process records
        time.sleep(5000);
        try {

            QueryResponse<List<StockTransactionAggregation>> rangeResult = queryForRangeResult(APP_ONE_PORT, SYMBOL_ONE, SYMBOL_TWO);
            List<String> expectedSymbols = List.of(SYMBOL_ONE);
            assertThat(expectedSymbols.size(), is(1) );
            assertThat(rangeResult.getResult().get(0), is(expectedSymbols.get(0)));

        } finally {
            contextOne.close();
            if (contextTwo.isRunning()) {
                contextTwo.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private QueryResponse<StockTransactionAggregation> queryForSingleResult(int port, String path) {
        QueryResponse<LinkedHashMap<String, Object>> response = restTemplate.getForObject("http://localhost:" + port + "/" + path, QueryResponse.class);
        String symbol = (String) response.getResult().get("symbol");
        double buys = (double) response.getResult().get("buys");
        double sells = (double) response.getResult().get("sells");
        QueryResponse<StockTransactionAggregation> queryResponse = QueryResponse.withResult(new StockTransactionAggregation(symbol, buys, sells));
        queryResponse.setHostType(response.getHostType());
        return queryResponse;
    }

    @SuppressWarnings("unchecked")
    private QueryResponse<List<StockTransactionAggregation>> queryForRangeResult(int port, String lower, String upper) {
        QueryResponse<List<LinkedHashMap<String, Object>>> response = restTemplate.getForObject("http://localhost:" + port + "/streams-iq/range?lower=" + lower + "&upper=" + upper, QueryResponse.class);
        List<StockTransactionAggregation> aggregationList = response.getResult().stream().map(lhm -> {
            String symbol = (String) lhm.get("symbol");
            double buys = (double) lhm.get("buys");
            double sells = (double) lhm.get("sells");
            return new StockTransactionAggregation(symbol, buys, sells);
        }).collect(Collectors.toList());
        return QueryResponse.withResult(aggregationList);
    }

    private ConfigurableApplicationContext createAndStartApplication(int serverPort, String bootstrapServers) {
        SpringApplicationBuilder applicationBuilder =
                new SpringApplicationBuilder(KafkaStreamsInteractiveQueriesApp.class);
        SpringApplication kafkaStreamsApp = applicationBuilder.build();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("server.port", serverPort);
        properties.put("secure.configs", "false");
        ConfigurableEnvironment env = new StandardEnvironment();
        env.setActiveProfiles("app-one-test-profile");

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
        Stream<StockTransaction> txnStream = Stream.generate(() -> builder.withSymbol(symbol).withAmount(100.00).withBuy(true).build());
        return txnStream.limit(numTransactions).collect(Collectors.toList());
    }

    private List<StockTransaction> getListForFilteredRange(String symbol, int numTransactions) {
        var builder = StockTransaction.StockTransactionBuilder.builder();
         if(symbol.equals("CFLT")) {
             Stream<StockTransaction> txnStream = Stream.generate(() -> builder.withSymbol("CFLT").withAmount(1000).withBuy(true).build());
             return txnStream.limit(5).collect(Collectors.toList());
         } else {
             return getTransactionList(symbol, numTransactions);
         }
    }


}
