package io.confluent.developer.streams;

import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransaction.StockTransactionBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class TestDataProducer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try(InputStreamReader inputStreamReader = new InputStreamReader( new FileInputStream("src/main/resources/confluent.properties")) ) {
            properties.load(inputStreamReader);
        }
        Random random = new Random();
        Time time = Time.SYSTEM;
        Serializer<StockTransaction> stockTransactionSerializer = SerdeUtil.stockTransactionSerde().serializer();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stockTransactionSerializer.getClass());

            try (KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(properties)) {
                while (true) {
                    List<String> keys = List.of("CFLT", "MSFT", "GOOG", "AACO", "AAPL", "ZELK");
                    StockTransactionBuilder builder = StockTransactionBuilder.builder();
                    List<StockTransaction> stockTransactionList = keys.stream().map(key -> builder.withSymbol(key)
                            .withAmount(random.nextDouble() * random.nextInt(500) * (random .nextBoolean() ? -1 : 1))
                            .withBuy(random.nextBoolean())
                            .build()).toList();
                    stockTransactionList.forEach(transaction -> producer.send(new ProducerRecord<>("input", transaction.getSymbol(), transaction), (meta, e) -> {
                        if (e != null) {
                            System.out.printf("Error producing %s %n", e);
                        } else {
                            System.out.printf("Produced record offset=%d, partition=%d, ts=%d %n", meta.offset(), meta.partition(), meta.timestamp());
                        }
                    }));
                    time.sleep(1000);
                }
            }
    }
}
