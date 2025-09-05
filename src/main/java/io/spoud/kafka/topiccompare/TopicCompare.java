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

        String outputFormat = getArg(args, "-o", getArg(args, "--output", "csv")).toLowerCase();
        boolean isCsv = outputFormat.equals("csv");
        boolean isJson = outputFormat.equals("json");
        if (!isCsv && !isJson) {
            System.err.println("Invalid output format: " + outputFormat + ". Use 'csv' or 'json'.");
            System.exit(1);
        }

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

        System.err.println("--- kafka-topic-compare configuration ---");
        System.err.println("Topic A: " + topicA + ", Bootstrap: " + propsA.getProperty("bootstrap.servers") + ", Properties: " + (clientPropsAPath != null ? clientPropsAPath : "(default)"));
        System.err.println("Topic B: " + topicB + ", Bootstrap: " + propsB.getProperty("bootstrap.servers") + ", Properties: " + (clientPropsBPath != null ? clientPropsBPath : "(default)"));
        System.err.println("Max messages: " + maxMessages);
        System.err.println("----------------------------------------");
        if (isCsv) {
            System.out.println("type,bootstrapA,topicA,partitionA,offsetA,bootstrapB,topicB,partitionB,offsetB");
        }
        DifferenceLogger logger = diff -> {
            String type = diff.getType().name();
            String bootstrapAVal = propsA.getProperty("bootstrap.servers", "");
            String bootstrapBVal = propsB.getProperty("bootstrap.servers", "");
            String topicAVal = diff.getRecordA() != null ? diff.getRecordA().topic() : "";
            String topicBVal = diff.getRecordB() != null ? diff.getRecordB().topic() : "";
            String partitionA = diff.getRecordA() != null ? String.valueOf(diff.getRecordA().partition()) : "";
            String partitionB = diff.getRecordB() != null ? String.valueOf(diff.getRecordB().partition()) : "";
            String offsetA = diff.getRecordA() != null ? String.valueOf(diff.getRecordA().offset()) : "";
            String offsetB = diff.getRecordB() != null ? String.valueOf(diff.getRecordB().offset()) : "";
            if (isCsv) {
                System.out.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                    type, bootstrapAVal, topicAVal, partitionA, offsetA, bootstrapBVal, topicBVal, partitionB, offsetB);
            } else {
                System.out.printf("{\"type\":\"%s\",\"bootstrapA\":\"%s\",\"topicA\":\"%s\",\"partitionA\":%s,\"offsetA\":%s,\"bootstrapB\":\"%s\",\"topicB\":\"%s\",\"partitionB\":%s,\"offsetB\":%s}\n",
                    type, escapeJson(bootstrapAVal), escapeJson(topicAVal),
                    partitionA.isEmpty() ? "null" : partitionA,
                    offsetA.isEmpty() ? "null" : offsetA,
                    escapeJson(bootstrapBVal), escapeJson(topicBVal),
                    partitionB.isEmpty() ? "null" : partitionB,
                    offsetB.isEmpty() ? "null" : offsetB);
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

    private static class ErrStreamHandler extends java.util.logging.StreamHandler {
        public ErrStreamHandler() {
            super(System.err, new java.util.logging.SimpleFormatter());
            setLevel(java.util.logging.Level.ALL);
        }
        @Override
        public synchronized void publish(java.util.logging.LogRecord record) {
            super.publish(record);
            flush();
        }
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
                // Redirect root appender to stderr
                Class<?> loggerContextClass = Class.forName("ch.qos.logback.classic.LoggerContext");
                Object loggerContext = Class.forName("org.slf4j.LoggerFactory").getMethod("getILoggerFactory").invoke(null);
                Class<?> appenderClass = Class.forName("ch.qos.logback.core.ConsoleAppender");
                Class<?> encoderClass = Class.forName("ch.qos.logback.classic.encoder.PatternLayoutEncoder");
                Object appender = appenderClass.getConstructor().newInstance();
                Object encoder = encoderClass.getConstructor().newInstance();
                encoderClass.getMethod("setContext", loggerContextClass).invoke(encoder, loggerContext);
                encoderClass.getMethod("setPattern", String.class).invoke(encoder, "%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n");
                encoderClass.getMethod("start").invoke(encoder);
                appenderClass.getMethod("setContext", loggerContextClass).invoke(appender, loggerContext);
                appenderClass.getMethod("setEncoder", encoderClass).invoke(appender, encoder);
                appenderClass.getMethod("setTarget", String.class).invoke(appender, "System.err");
                appenderClass.getMethod("start").invoke(appender);
                Object rootLogger = loggerContextClass.getMethod("getLogger", String.class).invoke(loggerContext, "ROOT");
                rootLogger.getClass().getMethod("detachAndStopAllAppenders").invoke(rootLogger);
                rootLogger.getClass().getMethod("addAppender", Class.forName("ch.qos.logback.core.Appender")).invoke(rootLogger, appender);
                System.err.println("Kafka logging set to " + (debug ? "DEBUG" : "WARN") + " (SLF4J/Logback, redirected to stderr)");
                return;
            }
        } catch (Throwable ignored) {}
        try {
            java.util.logging.Logger.getLogger("io.quarkus").setLevel(debug ? java.util.logging.Level.FINE : java.util.logging.Level.WARNING);
            java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(debug ? java.util.logging.Level.FINE : java.util.logging.Level.WARNING);
            java.util.logging.Logger.getLogger("org.apache.kafka.clients").setLevel(debug ? java.util.logging.Level.FINE : java.util.logging.Level.WARNING);
            java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
            java.util.logging.Handler[] handlers = rootLogger.getHandlers();
            for (java.util.logging.Handler handler : handlers) {
                if (handler instanceof java.util.logging.ConsoleHandler) {
                    rootLogger.removeHandler(handler);
                }
            }
            rootLogger.addHandler(new ErrStreamHandler());
        } catch (Throwable t) {
            System.err.println("Kafka logging: could not set log level or redirect to stderr (java.util.logging): " + t);
            t.printStackTrace(System.err);
            return;
        }
        System.err.println("Kafka logging set to " + (debug ? "FINE" : "WARNING") + " (java.util.logging, redirected to stderr)");
    }

    private static String escapeJson(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
