package io.confluent.developer.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.developer.config.KafkaStreamsAppConfiguration;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.store.CustomQueryStores;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class AggregationStream {

    private final KafkaStreamsAppConfiguration streamsConfiguration;

    @Autowired
    public AggregationStream(KafkaStreamsAppConfiguration streamsConfiguration) {
        this.streamsConfiguration = streamsConfiguration;
    }

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<StockTransaction> stockTransactionSerde = SerdeUtil.stockTransactionSerde();
    private final Serde<JsonNode> jsonNodeSerde = SerdeUtil.stockTransactionAggregateJsonNodeSerde();

    private final Initializer<JsonNode> initializer = () -> {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.put("sells", 0.0);
        objectNode.put("buys", 0.0);
        return objectNode;
    };

    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockTransaction> input = builder.stream(streamsConfiguration.inputTopic(),
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .peek((k, v) -> System.out.println("incoming" +
                        " key " + k + " value " + v));


        KeyValueBytesStoreSupplier persistentSupplier =
                CustomQueryStores.customPersistentStoreSupplier(streamsConfiguration.storeName());
        Materialized<String, JsonNode, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(persistentSupplier);
        materialized.withKeySerde(stringSerde).withValueSerde(jsonNodeSerde);

        input.groupByKey()
                .aggregate(initializer,
                        (k, v, agg) ->  {
                             ObjectNode objNode = (ObjectNode) agg;
                             if (!objNode.has("symbol")) {
                                 objNode.put("symbol", v.getSymbol());
                             }
                             if(v.getBuy()) {
                                 objNode.put("buys", objNode.get("buys").asDouble() + v.getAmount());
                             } else {
                                 objNode.put("sells", objNode.get("sells").asDouble() + v.getAmount());
                             }
                             return objNode;

                        }, materialized )
                .toStream()
                .peek((k, v) -> System.out.println("Aggregation result key " + k + " value " + v))
                .to(streamsConfiguration.outputTopic(), Produced.with(stringSerde, jsonNodeSerde));

        return builder.build();
    }
}
