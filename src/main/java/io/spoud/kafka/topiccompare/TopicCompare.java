package io.spoud.kafka.topiccompare;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

import java.util.Properties;

@QuarkusMain
public class TopicCompare implements QuarkusApplication {

    @Override
    public int run(String[] args) {
        if (hasArg(args, "--help")) {
            printHelp();
            return 0;
        }
        String bootstrapA = getArg(args, "--bootstrapA", "localhost:9092");
        String topicA = getArg(args, "--topicA", "topicA");
        String bootstrapB = getArg(args, "--bootstrapB", "localhost:9093");
        String topicB = getArg(args, "--topicB", "topicB");
        int maxMessages = Integer.parseInt(getArg(args, "--maxMessages", "1000"));
        String clientPropsAPath = getArg(args, "--clientPropertiesA", null);
        String clientPropsBPath = getArg(args, "--clientPropertiesB", null);

        boolean debug = hasArg(args, "--debug");
        configureKafkaLogging(debug);

        Properties propsA = new Properties();
        if (clientPropsAPath != null) {
            try (java.io.FileInputStream fis = new java.io.FileInputStream(clientPropsAPath)) {
                propsA.load(fis);
            } catch (Exception e) {
                System.err.println("Failed to load clientPropertiesA: " + e.getMessage());
                System.exit(1);
            }
        }
        propsA.putIfAbsent("bootstrap.servers", bootstrapA);
        propsA.putIfAbsent("group.id", "compare-a-" + System.currentTimeMillis());
        propsA.putIfAbsent("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.putIfAbsent("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.putIfAbsent("auto.offset.reset", "earliest");
        propsA.put("enable.auto.commit", "false");

        Properties propsB = new Properties();
        if (clientPropsBPath != null) {
            try (java.io.FileInputStream fis = new java.io.FileInputStream(clientPropsBPath)) {
                propsB.load(fis);
            } catch (Exception e) {
                System.err.println("Failed to load clientPropertiesB: " + e.getMessage());
                System.exit(1);
            }
        }
        propsB.putIfAbsent("bootstrap.servers", bootstrapB);
        propsB.putIfAbsent("group.id", "compare-b-" + System.currentTimeMillis());
        propsB.putIfAbsent("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.putIfAbsent("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.putIfAbsent("auto.offset.reset", "earliest");
        propsB.put("enable.auto.commit", "false");

        System.out.println("--- kafka-topic-compare configuration ---");
        System.out.println("Topic A: " + topicA + ", Bootstrap: " + propsA.getProperty("bootstrap.servers") + ", Properties: " + (clientPropsAPath != null ? clientPropsAPath : "(default)"));
        System.out.println("Topic B: " + topicB + ", Bootstrap: " + propsB.getProperty("bootstrap.servers") + ", Properties: " + (clientPropsBPath != null ? clientPropsBPath : "(default)"));
        System.out.println("Max messages: " + maxMessages);
        System.out.println("----------------------------------------");

        DifferenceLogger logger = diff -> {
            switch (diff.getType()) {
                case ONLY_IN_A -> System.out.println("Only in A: " + diff.getKeyHash());
                case ONLY_IN_B -> System.out.println("Only in B: " + diff.getKeyHash());
                case DUPLICATE_IN_A -> System.out.println("Duplicate in A: " + diff.getKeyHash());
                case DUPLICATE_IN_B -> System.out.println("Duplicate in B: " + diff.getKeyHash());
                case HEADER_DIFFERENCE -> System.out.println("Header difference: " + diff.getKeyHash());
            }
        };
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, maxMessages, logger);
        return 0;
    }

    private static boolean hasArg(String[] args, String name) {
        for (String arg : args) {
            if (arg.equals(name)) return true;
        }
        return false;
    }

    private static String getArg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static void printHelp() {
        System.out.println("kafka-topic-compare - Compare two Kafka topics for differences\n");
        System.out.println("Usage: java -jar kafka-topic-compare.jar [options]\n");
        System.out.println("Options:");
        System.out.println("  --bootstrapA <servers>         Kafka bootstrap servers for topic A (default: localhost:9092)");
        System.out.println("  --topicA <topic>               Name of topic A (default: topicA)");
        System.out.println("  --bootstrapB <servers>         Kafka bootstrap servers for topic B (default: localhost:9093)");
        System.out.println("  --topicB <topic>               Name of topic B (default: topicB)");
        System.out.println("  --maxMessages <n>              Maximum number of messages to compare (default: 1000)");
        System.out.println("  --clientPropertiesA <file>     Properties file for cluster A (optional, overrides defaults)");
        System.out.println("  --clientPropertiesB <file>     Properties file for cluster B (optional, overrides defaults)");
        System.out.println("  --help                         Show this help message and exit");
        System.out.println("  --debug                        Enable debug logging for Kafka");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -jar kafka-topic-compare.jar --bootstrapA localhost:9092 --topicA topicA --bootstrapB localhost:9093 --topicB topicB --maxMessages 100");
    }

    private static void configureKafkaLogging(boolean debug) {
        // Try SLF4J (Logback/Log4j) via reflection, avoid compile-time dependency
        try {
            org.slf4j.Logger kafkaLogger = org.slf4j.LoggerFactory.getLogger("org.apache.kafka");
            if (kafkaLogger.getClass().getName().equals("ch.qos.logback.classic.Logger")) {
                // Use reflection to set level
                Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
                Object level = levelClass.getField(debug ? "DEBUG" : "WARN").get(null);
                kafkaLogger.getClass().getMethod("setLevel", levelClass).invoke(kafkaLogger, level);
                org.slf4j.Logger kafkaClientsLogger = org.slf4j.LoggerFactory.getLogger("org.apache.kafka.clients");
                kafkaClientsLogger.getClass().getMethod("setLevel", levelClass).invoke(kafkaClientsLogger, level);
                System.out.println("Kafka logging set to " + (debug ? "DEBUG" : "WARN") + " (SLF4J/Logback)");
                return;
            }
        } catch (Throwable ignored) {}
        // Fallback: java.util.logging
        try {
            java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(debug ? java.util.logging.Level.FINE : java.util.logging.Level.WARNING);
            java.util.logging.Logger.getLogger("org.apache.kafka.clients").setLevel(debug ? java.util.logging.Level.FINE : java.util.logging.Level.WARNING);
            System.out.println("Kafka logging set to " + (debug ? "FINE" : "WARNING") + " (java.util.logging)");
            return;
        } catch (Throwable ignored) {}
        System.out.println("Kafka logging: could not set log level (no supported backend found)");
    }
}
