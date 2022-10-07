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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


@Testcontainers
public class InteractiveQueriesTest {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Time time = Time.SYSTEM;
    private final NewTopic inputTopic = new NewTopic("input", 2, (short) 1);
    private final NewTopic outputTopic = new NewTopic("output", 2, (short) 1);
    private final List<NewTopic> topics = List.of(inputTopic, outputTopic);


    @Container
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.0"));



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
    @DisplayName("Test of IQ with Standby Tasks")
    public void testStandbyKeyQueryIQ() {
        produceInputRecords();
        SpringApplicationBuilder ksAppOneBuilder =
                new SpringApplicationBuilder(KafkaStreamsInteractiveQueriesApp.class);
        SpringApplication ksAppOne = ksAppOneBuilder.build();
        Properties properties = new Properties();
        properties.put("bootstrap.servers",kafka.getBootstrapServers());
        properties.put("server.port", 7090);
        properties.put("secure.configs", "false");
        ConfigurableEnvironment env = new StandardEnvironment();
        env.setActiveProfiles("app-one-test-profile");

        env.getPropertySources()
                .addFirst(new PropertiesPropertySource("initProps", properties));

        ksAppOne.setEnvironment(env);
        ConfigurableApplicationContext contextOne = ksAppOne.run();



        SpringApplicationBuilder ksAppTwo =
                new SpringApplicationBuilder(KafkaStreamsInteractiveQueriesApp.class)
                        .properties("bootstrap.servers="+kafka.getBootstrapServers()+"",
                                "server.port=7077");
        time.sleep(8000);
       // ConfigurableApplicationContext contextTwo = ksAppTwo.run();
       // time.sleep(3000);
        System.out.println("Started both applications");
        try {
            QueryResponse<StockTransactionAggregation> appOneResult = restTemplate.getForObject("http://localhost:7090/streams-iq/keyquery/CFLT", QueryResponse.class);
            System.out.println(appOneResult);
        } finally {
            contextOne.close();
        }

        // QueryResponse<StockTransactionAggregation> appTwoResult = restTemplate.getForObject("http://localhost:7077/streams-ig/keyquery/CFLT", QueryResponse.class);
        // System.out.println(appTwoResult);
    }

    private void produceInputRecords() {
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Object> producerConfigs = Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", StringSerializer.class,
                "value.serializer", SerdeUtil.stockTransactionSerde().serializer().getClass());
        try(KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            getTransactionList().forEach(txn -> producer.send(new ProducerRecord<>("input",counter.getAndIncrement() % 2, txn.getSymbol(), txn), ((metadata, exception) -> {
                if(exception!=null) {
                    System.err.println("Error producing records" + exception.getMessage());
                }else {
                    System.out.println(String.format("Produced record with offset %d and partition %d", metadata.offset(), metadata.partition()));
                }
            })));
        }
    }

    private List<StockTransaction> getTransactionList() {
        List<StockTransaction> transactions = new ArrayList<>();
        var builder = StockTransaction.StockTransactionBuilder.builder();
        transactions.add(builder.withSymbol("CFLT").withAmount(100.00).withBuy(false).build());
        transactions.add(builder.withSymbol("AABB").withAmount(300.00).withBuy(true).build());
        transactions.add(builder.withSymbol("CFLT").withAmount(500.00).withBuy(true).build());
        transactions.add(builder.withSymbol("AABB").withAmount(200.00).withBuy(true).build());
        return transactions;
    }


}
