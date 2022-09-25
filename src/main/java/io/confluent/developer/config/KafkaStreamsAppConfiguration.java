package io.confluent.developer.config;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Component
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

    public Properties streamsConfigs() {
        Map<String, Object> streamsConfigs = new HashMap<>();
        streamsConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfigs.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheConfig);
        streamsConfigs.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
        Properties properties = new Properties();
        properties.putAll(streamsConfigs);
        properties.putAll(saslConfigs());
        return properties;
    }

    private Properties saslConfigs() {
        Properties properties = new Properties();
        try (InputStream is = KafkaStreamsAppConfiguration.class.getClassLoader().getResourceAsStream("confluent.properties")) {
            properties.load(is);
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
}
