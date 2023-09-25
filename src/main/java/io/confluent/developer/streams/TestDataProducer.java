package io.confluent.developer.streams;

import io.confluent.developer.model.StockTransaction;
import io.confluent.developer.model.StockTransaction.StockTransactionBuilder;
import net.datafaker.Faker;
import net.datafaker.providers.base.Bool;
import net.datafaker.providers.base.Number;
import net.datafaker.providers.base.Stock;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class TestDataProducer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream("src/main/resources/confluent.properties"))) {
            properties.load(inputStreamReader);
        }
        Time time = Time.SYSTEM;
        Serializer<StockTransaction> stockTransactionSerializer = SerdeUtil.stockTransactionSerde().serializer();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stockTransactionSerializer.getClass());
        Faker faker = new Faker();
        Stock stockFaker = faker.stock();
        Number numberFaker = faker.number();
        Bool booleanFaker = faker.bool();
        List<String> keys = List.of("CFLT", "ZELK");
        int numberIterations = Integer.parseInt(properties.getProperty("number.iterations", Integer.toString(1000)));
        AtomicInteger produceCount = new AtomicInteger(0);
        List<String> randomKeys = new ArrayList<>();

        List<String> baseSymbols = Stream.generate(stockFaker::nsdqSymbol).limit(200).toList();
        List<String> tickerSymbols = new ArrayList<>(baseSymbols);
        tickerSymbols.addAll(keys);
        AtomicInteger partitionCounter = new AtomicInteger(1);

        for (int i = 0; i < 4; i++) {
            randomKeys.add(tickerSymbols.get(numberFaker.numberBetween(0, tickerSymbols.size())));
        }


        try (KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(properties)) {
            while (produceCount.getAndIncrement() < numberIterations) {
                StockTransactionBuilder builder = StockTransactionBuilder.builder();
                tickerSymbols.stream().map(key -> {
                            builder.withSymbol(key)
                                        .withAmount(numberFaker.randomDouble(2, 1, 5))
                                    .withBuy(booleanFaker.bool());
                            if (randomKeys.contains(key)) {
                                builder.withNumberShares(numberFaker.numberBetween(8000, 15000));
                                if (produceCount.get() <= 7) {
                                    builder.withBuy(true);
                                }
                            } else {
                                builder.withNumberShares(numberFaker.numberBetween(1000, 3000));
                            }
                        return  builder.build();
                        }
                ).forEach(transaction -> producer.send(new ProducerRecord<>("input", partitionCounter.incrementAndGet() % 2, transaction.getSymbol(), transaction), (meta, e) -> {
                            if (e != null) {
                                System.out.printf("Error producing %s %n", e);
                            } else {
                                System.out.printf("Produced record offset=%d, partition=%d, ts=%d %n", meta.offset(), meta.partition(), meta.timestamp());
                            }
                        })
                );
                time.sleep(1000);
            }
        }
        System.out.printf("Produce count of [%d] reached quitting now%n", produceCount.get());
    }
}
