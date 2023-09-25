package io.confluent.developer.streams;

import io.confluent.developer.config.KafkaStreamsAppConfiguration;
import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.store.CustomQueryStores;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Properties;

@Component
public class KafkaStreamsService {
    private final KafkaStreamsAppConfiguration appConfiguration;
    private KafkaStreams kafkaStreams;
    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<StockTransaction> stockTransactionSerde = SerdeUtil.stockTransactionSerde();
    private final Serde<StockTransactionAggregationProto> protoSerde = SerdeUtil.stockTransactionAggregationProtoJsonSerde();
    private final Initializer<StockTransactionAggregationProto> initializer = () -> StockTransactionAggregationProto.newBuilder().build();


    @Autowired
    public KafkaStreamsService(final KafkaStreamsAppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
    }
    @Bean
    public KafkaStreams kafkaStreams() {
        return kafkaStreams;
    }

    @PostConstruct
    public void init() {
        kafkaStreams = new KafkaStreams(topology(), appConfiguration.streamsConfigs());
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    @PreDestroy
    public void tearDown(){
        kafkaStreams.close(Duration.ofSeconds(10));
    }

    private final Aggregator<String, StockTransaction, StockTransactionAggregationProto> aggregator = (key, value, agg) -> {
        StockTransactionAggregationProto.Builder builder = agg.toBuilder();
        boolean isBuy = value.getBuy();
        if (isBuy) {
            builder.setBuys(value.getAmount() + builder.getBuys());
        } else {
            builder.setSells(value.getAmount() + builder.getSells());
        }
        builder.setNumberShares(value.getNumberShares() + builder.getNumberShares());
        
        builder.setSymbol(value.getSymbol());
        return builder.build();
    };

    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockTransaction> input = builder.stream(appConfiguration.inputTopic(),
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .peek((k, v) -> System.out.println("incoming" +
                        " key " + k + " value " + v));

        KeyValueBytesStoreSupplier persistentSupplier =
                CustomQueryStores.customPersistentStoreSupplier(appConfiguration.storeName());
        Materialized<String, StockTransactionAggregationProto, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(persistentSupplier);
        materialized.withKeySerde(stringSerde).withValueSerde(protoSerde);

        input.groupByKey()
                .aggregate(initializer, aggregator, materialized)
                .toStream()
                .peek((k, v) -> System.out.println("Aggregation result key " + k + " value " + v))
                .to(appConfiguration.outputTopic(), Produced.with(stringSerde, protoSerde));

        return builder.build();
    }
}
