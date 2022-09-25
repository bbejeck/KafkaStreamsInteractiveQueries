package io.confluent.developer.streams;

import io.confluent.developer.config.KafkaStreamsAppConfiguration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Properties;

@Component
public class KafkaStreamsContainer {
    private final KafkaStreamsAppConfiguration appConfiguration;
    private final AggregationStream aggregationStream;
    private KafkaStreams kafkaStreams;

    @Autowired
    public KafkaStreamsContainer(final AggregationStream aggregationStream,
                                 final KafkaStreamsAppConfiguration appConfiguration) {
        this.aggregationStream = aggregationStream;
        this.appConfiguration = appConfiguration;
    }
    @Bean
    public KafkaStreams kafkaStreams() {
        return kafkaStreams;
    }

    @PostConstruct
    public void init() {
        Properties properties = appConfiguration.streamsConfigs();
        Topology topology = aggregationStream.topology();
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    @PreDestroy
    public void tearDown(){
        kafkaStreams.close(Duration.ofSeconds(10));
    }
}
