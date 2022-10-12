package io.confluent.developer.controller;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Scanner;

/**
 * This class generates the partition for a given string.
 * Useful when trying to develop some random string keys, and you want to
 * ensure the provided keys will distribute evenly
 */
public class PartitionGenerator {

    public static void main(String[] args) {
        String key = "";
        Scanner scanner = new Scanner(System.in);
        try (StringSerializer stringSerializer = new StringSerializer()) {
            while (!key.equals("q")) {
                key = scanner.nextLine();
                byte[] keyBytes = stringSerializer.serialize(null, key);
                System.out.println(Utils.toPositive(Utils.murmur2(keyBytes)) % 2);
            }
        }
    }
}
