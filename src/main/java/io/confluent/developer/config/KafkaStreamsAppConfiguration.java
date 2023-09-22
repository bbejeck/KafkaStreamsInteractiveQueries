package io.confluent.developer.config;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import net.datafaker.providers.base.Stock;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Component
@PropertySource("classpath:application.properties")
public class KafkaStreamsAppConfiguration {
    @Value("${application.id}")
    private String applicationId;

    @Value("${max.cache.config}")
    private int cacheConfig;

    @Value("${application.server}")
    private String applicationServer;

    @Value("${store.name}")
    private String storeName;

    @Value("${input.topic.name}")
    private String inputTopic;

    @Value("${output.topic.name}")
    private String outputTopic;

    @Value("${bootstrap.servers}")
    private List<String> bootstrapServers;

    @Value("${server.port}")
    private int serverPort;

    @Value("${grpc.port}")
    private int grpcPort;

    @Value("${secure.configs}")
    private String secureConfigs;

    @Value("${persistent.metrics.scope")
    private String persistentMetricsScope;



    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jackson2ObjectMapperBuilderCustomizer() {
        JsonFormat.TypeRegistry typeRegistry = JsonFormat.TypeRegistry.newBuilder()
                .add(StockTransactionAggregationProto.getDescriptor())
                .build();

        JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry)
                .includingDefaultValueFields();

        return objectMapperBuilder ->
                objectMapperBuilder.serializerByType(Message.class, new JsonSerializer<Message>() {
                    @Override
                    public void serialize(Message message,
                                          JsonGenerator jsonGenerator,
                                          SerializerProvider serializers) throws IOException {
                        jsonGenerator.writeRawValue(printer.print(message));
                    }
                });
    }


    public Properties streamsConfigs() {
        Map<String, Object> streamsConfigs = new HashMap<>();
        Properties properties = new Properties();
        streamsConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfigs.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, cacheConfig);
        streamsConfigs.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
        streamsConfigs.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        // This is set intentionally higher than normal to facilitate the demo we don't want
        // a rebalance to occur and reassign the standby to active too soon
        streamsConfigs.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 60 * 1000);
        String stateDirConfig = System.getProperty("java.io.tmpdir") + File.separator + "kafka-streams-" + serverPort;
        streamsConfigs.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfigs.put(StreamsConfig.STATE_DIR_CONFIG, stateDirConfig);
        properties.putAll(streamsConfigs);
        if(secureConfigs.equals("true")) {
            properties.putAll(saslConfigs());
        }
        return properties;
    }

    private Properties saslConfigs() {
        Properties properties = new Properties();
        try (InputStream is = KafkaStreamsAppConfiguration.class.getClassLoader().getResourceAsStream("confluent.properties")) {
            if (is != null) {
                properties.load(is);
            }
            return properties;
        } catch (IOException e) {
            System.out.println("For secure connections make sure to have confluent.properties file in src/main/resources");
            return properties;
        }
    }

    public String storeName() {
        return storeName;
    }

    public String inputTopic() {
        return inputTopic;
    }

    public String outputTopic() {
        return outputTopic;
    }

    public String persistentMetricsScope() {
        return persistentMetricsScope;
    }
}
