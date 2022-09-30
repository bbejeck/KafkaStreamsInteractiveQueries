package io.confluent.developer.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/status")
public class StatusController {

    private final KafkaStreams kafkaStreams;
    private final RestTemplate restTemplate;

    @Value("${application.server}")
    private String applicationServer;

    private static final String STATUS_URL = "http://{host}:{port}/status";

    @Autowired
    public StatusController(KafkaStreams kafkaStreams, RestTemplateBuilder restTemplateBuilder) {
        this.kafkaStreams = kafkaStreams;
        this.restTemplate = restTemplateBuilder.build();
    }

    @GetMapping(value = "/all")
    public Map<String, String> allHostStats() {
        Map<String, String> allHostStatus = new HashMap<>();
        allHostStatus.put(applicationServer, "ALIVE");
        Collection<StreamsMetadata> allStreamsMetadata = kafkaStreams.metadataForAllStreamsClients();
        allStreamsMetadata.forEach(metadata -> {
             String hostStatus = doRemoteRequest(metadata.host(), metadata.port(), "/heartbeat");
             allHostStatus.put(metadata.host()+":"+metadata.port(), hostStatus);

        });
      return allHostStatus;
    }

    @GetMapping("/heartbeat")
    public String heartbeat() {
          return "ACTIVE";
    }

    private String doRemoteRequest(String host, int port, String path) {
        String status;
        try {
            status = restTemplate.getForObject(STATUS_URL + path, String.class, host, port);
        } catch (RestClientException exception) {
            status = "DEAD";
        }
        return status;
    }
}
