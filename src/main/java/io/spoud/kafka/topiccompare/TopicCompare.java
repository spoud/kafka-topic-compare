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
        // Summary tracking variables
        final long[] minOffsetA = {Long.MAX_VALUE};
        final long[] maxOffsetA = {Long.MIN_VALUE};
        final long[] minOffsetB = {Long.MAX_VALUE};
        final long[] maxOffsetB = {Long.MIN_VALUE};
        final int[] diffCount = {0};
        // Track offsets per partition and diff type counts
        final java.util.Map<Integer, long[]> offsetsA = new java.util.TreeMap<>(); // partition -> [min, max]
        final java.util.Map<Integer, long[]> offsetsB = new java.util.TreeMap<>();
        final java.util.EnumMap<Difference.Type, Integer> diffTypeCounts = new java.util.EnumMap<>(Difference.Type.class);
        for (Difference.Type t : Difference.Type.values()) diffTypeCounts.put(t, 0);
        boolean printDiffDetails = hasArg(args, "--print-diff");
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
            if (printDiffDetails) {
                // Print detailed diff to stderr, indented
                String indent = "    ";
                // 1. Headers
                String headersA = diff.getRecordA() != null ? headersToString(diff.getRecordA().headers(), isCsv) : "";
                String headersB = diff.getRecordB() != null ? headersToString(diff.getRecordB().headers(), isCsv) : "";
                String timestampA = diff.getRecordA() != null ? String.valueOf(diff.getRecordA().timestamp()) : "";
                String timestampB = diff.getRecordB() != null ? String.valueOf(diff.getRecordB().timestamp()) : "";
                System.err.println(indent + "timestamp: " + timestampA + " <-> " + timestampB);
                System.err.println(indent + "headers: " + headersA + " <-> " + headersB);
                // 2. Key (base64)
                String keyA = diff.getRecordA() != null && diff.getRecordA().key() != null ? java.util.Base64.getEncoder().encodeToString(diff.getRecordA().key()) : "";
                String keyB = diff.getRecordB() != null && diff.getRecordB().key() != null ? java.util.Base64.getEncoder().encodeToString(diff.getRecordB().key()) : "";
                System.err.println(indent + "key:    " + keyA + " <-> " + keyB);
                // 3. Value (base64)
                String valueA = diff.getRecordA() != null && diff.getRecordA().value() != null ? java.util.Base64.getEncoder().encodeToString(diff.getRecordA().value()) : "";
                String valueB = diff.getRecordB() != null && diff.getRecordB().value() != null ? java.util.Base64.getEncoder().encodeToString(diff.getRecordB().value()) : "";
                System.err.println(indent + "value:  " + valueA + " <-> " + valueB);
            }
            if (diff.getRecordA() != null) {
                int part = diff.getRecordA().partition();
                long off = diff.getRecordA().offset();
                offsetsA.compute(part, (k, v) -> v == null ? new long[]{off, off} : new long[]{Math.min(v[0], off), Math.max(v[1], off)});
            }
            if (diff.getRecordB() != null) {
                int part = diff.getRecordB().partition();
                long off = diff.getRecordB().offset();
                offsetsB.compute(part, (k, v) -> v == null ? new long[]{off, off} : new long[]{Math.min(v[0], off), Math.max(v[1], off)});
            }
            diffTypeCounts.put(diff.getType(), diffTypeCounts.get(diff.getType()) + 1);
            if (diff.getRecordA() != null) {
                long off = diff.getRecordA().offset();
                if (off < minOffsetA[0]) minOffsetA[0] = off;
                if (off > maxOffsetA[0]) maxOffsetA[0] = off;
            }
            if (diff.getRecordB() != null) {
                long off = diff.getRecordB().offset();
                if (off < minOffsetB[0]) minOffsetB[0] = off;
                if (off > maxOffsetB[0]) maxOffsetB[0] = off;
            }
            diffCount[0]++;
        };
        String startTimestampIso = getArg(args, "--startTimestamp", null);
        Long startTimestamp = null;
        if (startTimestampIso != null) {
            try {
                // Try ISO 8601 first
                java.time.Instant instant = java.time.Instant.parse(startTimestampIso);
                startTimestamp = instant.toEpochMilli();
            } catch (Exception isoEx) {
                try {
                    // Try epoch millis
                    startTimestamp = Long.parseLong(startTimestampIso);
                } catch (Exception millisEx) {
                    System.err.println("Invalid timestamp for --startTimestamp: " + startTimestampIso + ". Use ISO 8601 or epoch millis.");
                    System.exit(1);
                }
            }
        }
        // Parse --skip-header option

        String skipHeaderDiffArg = getArg(args, "--skip-header", null);
        java.util.Set<String> skipHeaderNames = null;
        boolean disableHeaderComparison = hasArg(args, "--skip-header");;
        if (skipHeaderDiffArg != null) {
            if (!skipHeaderDiffArg.trim().isEmpty()) {
                skipHeaderNames = new java.util.HashSet<>();
                for (String h : skipHeaderDiffArg.split(",")) {
                    if (!h.trim().isEmpty()) skipHeaderNames.add(h.trim());
                }
            }
        }
        TopicCompareService service = new TopicCompareService();
        CompareResult compareResult = service.compareTopics(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, skipHeaderNames, disableHeaderComparison);
        // Print summary to stderr
        System.err.println("--- Summary ---");
        System.err.println("Differences (rows): " + diffCount[0]);
        for (Difference.Type t : Difference.Type.values()) {
            System.err.println("  " + t + ": " + diffTypeCounts.get(t));
        }
        System.err.println("Topic A partition offsets:");
        if (compareResult.statsA.startOffsets == null || compareResult.statsA.startOffsets.isEmpty()) {
            System.err.println("  (no data in A)");
        } else {
            for (var e : compareResult.statsA.startOffsets.entrySet()) {
                System.err.println("  Partition " + e.getKey().partition() + ": start=" + e.getValue() + ", end=" + compareResult.statsA.endOffsets.getOrDefault(e.getKey(), -1L));
            }
        }
        System.err.println("Topic B partition offsets:");
        if (compareResult.statsB.startOffsets == null || compareResult.statsB.startOffsets.isEmpty()) {
            System.err.println("  (no data in B)");
        } else {
            for (var e : compareResult.statsB.startOffsets.entrySet()) {
                System.err.println("  Partition " + e.getKey().partition() + ": start=" + e.getValue() + ", end=" + compareResult.statsB.endOffsets.getOrDefault(e.getKey(), -1L));
            }
        }
        System.err.println("A: eventsRead=" + compareResult.statsA.eventsRead + ", endReached=" + compareResult.statsA.endReached + ", timestampsMonotonous=" + compareResult.statsA.timestampsMonotonous);
        System.err.println("B: eventsRead=" + compareResult.statsB.eventsRead + ", endReached=" + compareResult.statsB.endReached + ", timestampsMonotonous=" + compareResult.statsB.timestampsMonotonous);
        System.err.println("----------------");
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
        System.out.println("  --startTimestamp <timestamp>   Start timestamp for message comparison (ISO 8601 format or epoch milliseconds, optional)");
        System.out.println("  --help                         Show this help message and exit");
        System.out.println("  --debug                        Enable debug logging for Kafka");
        System.out.println("  --print-diff                   Print detailed diff (headers, key, value as base64) indented to each diff, on stderr");
        System.out.println("  --skip-header <headers>        Optional comma-separated list of header names to skip in diff (or empty to disable header comparison)");
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

    // Helper to format headers as CSV or JSON
    private static String headersToString(org.apache.kafka.common.header.Headers headers, boolean isCsv) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (org.apache.kafka.common.header.Header h : headers) {
            if (!first) sb.append(isCsv ? ";" : ", ");
            sb.append(h.key()).append("=");
            sb.append(h.value() != null ? java.util.Base64.getEncoder().encodeToString(h.value()) : "null");
            first = false;
        }
        return sb.toString();
    }
}
