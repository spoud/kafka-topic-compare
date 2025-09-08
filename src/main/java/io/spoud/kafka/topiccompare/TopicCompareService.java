package io.spoud.kafka.topiccompare;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.time.Duration;

public class TopicCompareService {
    // Unified compareTopics implementation
    private void compareTopicsImpl(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        try (KafkaConsumer<byte[], byte[]> consumerA = new KafkaConsumer<>(propsA);
             KafkaConsumer<byte[], byte[]> consumerB = new KafkaConsumer<>(propsB)) {

            consumerA.subscribe(Collections.singletonList(topicA));
            consumerB.subscribe(Collections.singletonList(topicB));

            // Seek to startTimestamp if provided
            if (startTimestamp != null) {
                consumerA.poll(Duration.ofMillis(100));
                consumerB.poll(Duration.ofMillis(100));
                Set<org.apache.kafka.common.TopicPartition> partitionsA = consumerA.assignment();
                Set<org.apache.kafka.common.TopicPartition> partitionsB = consumerB.assignment();
                while (partitionsA.isEmpty() || partitionsB.isEmpty()) {
                    consumerA.poll(Duration.ofMillis(100));
                    consumerB.poll(Duration.ofMillis(100));
                    partitionsA = consumerA.assignment();
                    partitionsB = consumerB.assignment();
                }
                Map<org.apache.kafka.common.TopicPartition, Long> timestampMapA = new HashMap<>();
                for (org.apache.kafka.common.TopicPartition tp : partitionsA) {
                    timestampMapA.put(tp, startTimestamp);
                }
                Map<org.apache.kafka.common.TopicPartition, Long> timestampMapB = new HashMap<>();
                for (org.apache.kafka.common.TopicPartition tp : partitionsB) {
                    timestampMapB.put(tp, startTimestamp);
                }
                Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsA = consumerA.offsetsForTimes(timestampMapA);
                for (var entry : offsetsA.entrySet()) {
                    if (entry.getValue() != null) {
                        consumerA.seek(entry.getKey(), entry.getValue().offset());
                    }
                }
                Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsB = consumerB.offsetsForTimes(timestampMapB);
                for (var entry : offsetsB.entrySet()) {
                    if (entry.getValue() != null) {
                        consumerB.seek(entry.getKey(), entry.getValue().offset());
                    }
                }
            }

            Map<String, ConsumerRecord<byte[], byte[]>> recordsA = new HashMap<>();
            Map<String, ConsumerRecord<byte[], byte[]>> recordsB = new HashMap<>();

            Set<String> seenA = new HashSet<>();
            Set<String> seenB = new HashSet<>();
            List<String> orderA = new ArrayList<>();
            List<String> orderB = new ArrayList<>();
            // Helper to create a unique string from key only (for compacted) or key+value (for normal)
            java.util.function.BiFunction<byte[], byte[], String> keyHash = (keyBytes, valueBytes) -> {
                if (keyBytes != null) {
                    return java.util.Base64.getEncoder().encodeToString(keyBytes);
                } else if (valueBytes != null) {
                    return "null:" + java.util.Arrays.hashCode(valueBytes);
                } else {
                    return "null:null";
                }
            };
            // Helper to create a unique string from key, value, and timestamp for duplicate detection
            java.util.function.Function<ConsumerRecord<byte[], byte[]>, String> duplicateKey = record -> {
                return java.util.Base64.getEncoder().encodeToString(record.key() == null ? new byte[0] : record.key()) +
                        ":" + java.util.Base64.getEncoder().encodeToString(record.value() == null ? new byte[0] : record.value()) +
                        ":" + record.timestamp();
            };
            boolean reachedEndA = false;
            boolean reachedEndB = false;
            long lastTimestampA = -1;
            long lastTimestampB = -1;
            boolean monotonicTimestampsA = true;
            boolean monotonicTimestampsB = true;
            // Read messages from topic A
            int countA = 0;
            while (countA < maxMessages) {
                var records = consumerA.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    reachedEndA = true;
                    break;
                }
                for (var record : records) {
                    String key = keyHash.apply(record.key(), compacted ? null : record.value());
                    lastTimestampA = Math.max(record.timestamp(), lastTimestampA);
                    if (record.timestamp() < lastTimestampA) {
                        monotonicTimestampsA = false;
                    }
                    if (compacted) {
                        // Always keep the latest for this key
                        recordsA.put(key, record);
                        if (!seenA.contains(key)) orderA.add(key);
                        seenA.add(key);
                    } else {
                        String dupKey = duplicateKey.apply(record);
                        if (seenA.contains(dupKey)) {
                            logger.log(new Difference(Difference.Type.DUPLICATE_IN_A, record, null, key));
                        } else {
                            seenA.add(dupKey);
                            recordsA.put(dupKey, record);
                            orderA.add(dupKey);
                        }
                    }
                    countA++;
                    if (countA >= maxMessages) break;
                }
            }

            // Read messages from topic B
            int countB = 0;
            while (countB < maxMessages) {
                var records = consumerB.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    reachedEndB = true;
                    break;
                }
                for (var record : records) {
                    String key = keyHash.apply(record.key(), compacted ? null : record.value());
                    lastTimestampB = Math.max(record.timestamp(), lastTimestampB);
                    if (record.timestamp() < lastTimestampA) {
                        monotonicTimestampsB = false;
                    }
                    if (compacted) {
                        recordsB.put(key, record);
                        if (!seenB.contains(key)) orderB.add(key);
                        seenB.add(key);
                    } else {
                        String dupKey = duplicateKey.apply(record);
                        if (seenB.contains(dupKey)) {
                            logger.log(new Difference(Difference.Type.DUPLICATE_IN_B, null, record, key));
                        } else {
                            seenB.add(dupKey);
                            recordsB.put(dupKey, record);
                            orderB.add(dupKey);
                        }
                    }
                    countB++;
                    if (countB >= maxMessages) break;
                }
            }

            // Compare
            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(recordsA.keySet());
            allKeys.addAll(recordsB.keySet());

            // Identify the last contiguous block of missing records at the end for A
            Set<String> missingAtEndA = new HashSet<>();
            if (!orderA.isEmpty()) {
                int idx = orderA.size() - 1;
                while (idx >= 0) {
                    String key = orderA.get(idx);
                    if (recordsB.containsKey(key)) {
                        break;
                    }
                    missingAtEndA.add(key);
                    idx--;
                }
            }
            // Identify the last contiguous block of missing records at the end for B
            Set<String> missingAtEndB = new HashSet<>();
            if (!orderB.isEmpty()) {
                int idx = orderB.size() - 1;
                while (idx >= 0) {
                    String key = orderB.get(idx);
                    if (recordsA.containsKey(key)) {
                        break;
                    }
                    missingAtEndB.add(key);
                    idx--;
                }
            }

            for (String key : allKeys) {
                boolean inA = recordsA.containsKey(key);
                boolean inB = recordsB.containsKey(key);
                if (inA && inB) {
                    ConsumerRecord<byte[], byte[]> recA = recordsA.get(key);
                    ConsumerRecord<byte[], byte[]> recB = recordsB.get(key);
                    if (!headersEqual(recA, recB, skipHeaderNames, disableHeaderComparison)) {
                        if (!disableHeaderComparison) {
                            logger.log(new Difference(Difference.Type.HEADER_DIFFERENCE, recA, recB, key));
                        }
                    }
                } else if (inA && !inB) {
                    if (missingAtEndA.contains(key)) {
                        logger.log(new Difference(Difference.Type.MISSING_AT_END, recordsA.get(key), null, key));
                    } else {
                        logger.log(new Difference(Difference.Type.ONLY_IN_A, recordsA.get(key), null, key));
                    }
                } else if (!inA && inB) {
                    if (missingAtEndB.contains(key)) {
                        logger.log(new Difference(Difference.Type.MISSING_AT_END, null, recordsB.get(key), key));
                    } else {
                        logger.log(new Difference(Difference.Type.ONLY_IN_B, null, recordsB.get(key), key));
                    }
                }
            }

            if (!compacted) {
                // Out-of-order detection (not relevant for compacted topics)
                for (String key : allKeys) {
                    if (recordsA.containsKey(key) && recordsB.containsKey(key)) {
                        int idxA = orderA.indexOf(key);
                        int idxB = orderB.indexOf(key);
                        if (idxA != -1 && idxB != -1 && idxA != idxB) {
                            logger.log(new Difference(Difference.Type.OUT_OF_ORDER, recordsA.get(key), recordsB.get(key), key));
                        }
                    }
                }
            }
        }
    }

    // Helper for config diff and compacted detection
    private boolean isCompactedAndLogConfigDiff(Properties propsA, String topicA, Properties propsB, String topicB) {
        boolean compacted = false;
        try (AdminClient adminA = AdminClient.create(propsA); AdminClient adminB = AdminClient.create(propsB)) {
            ConfigResource resA = new ConfigResource(ConfigResource.Type.TOPIC, topicA);
            ConfigResource resB = new ConfigResource(ConfigResource.Type.TOPIC, topicB);
            DescribeConfigsResult resultA = adminA.describeConfigs(Collections.singleton(resA));
            DescribeConfigsResult resultB = adminB.describeConfigs(Collections.singleton(resB));
            Config configA = resultA.all().get().get(resA);
            Config configB = resultB.all().get().get(resB);
            String cleanupA = configA.get("cleanup.policy") != null ? configA.get("cleanup.policy").value() : null;
            String cleanupB = configB.get("cleanup.policy") != null ? configB.get("cleanup.policy").value() : null;
            if ((cleanupA != null && cleanupA.contains("compact")) || (cleanupB != null && cleanupB.contains("compact"))) {
                compacted = true;
                System.err.println("INFO Detected compacted topic (cleanup.policy): " + topicA + " (" + propsA.getProperty("bootstrap.servers") + ") and/or " + topicB + " (" + propsB.getProperty("bootstrap.servers") + ")");
            }
            // Diff and log all configs
            Set<String> allProps = new TreeSet<>();
            configA.entries().forEach(e -> allProps.add(e.name()));
            configB.entries().forEach(e -> allProps.add(e.name()));
            for (String prop : allProps.stream().sorted().toList()) {
                String valA = configA.get(prop) != null ? configA.get(prop).value() : "";
                String valB = configB.get(prop) != null ? configB.get(prop).value() : "";
                if (!Objects.equals(valA, valB)) {
                    System.err.println("WARNING Difference in topic configuration:" + prop + "," + propsA.getProperty("bootstrap.servers", "A") + "," + valA + "," + propsB.getProperty("bootstrap.servers", "B") + "," + valB);
                }
            }
        } catch (Exception e) {
            compacted = false;
        }
        return compacted;
    }

    // Public API: original signatures, now all delegate to unified implementation
    public void compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp) {
        boolean compacted = isCompactedAndLogConfigDiff(propsA, topicA, propsB, topicB);
        compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, null, false);
    }

    public void compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted) {
        compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, null, false);
    }

    public void compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        boolean compacted = isCompactedAndLogConfigDiff(propsA, topicA, propsB, topicB);
        compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, skipHeaderNames, disableHeaderComparison);
    }

    public void compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, skipHeaderNames, disableHeaderComparison);
    }

    // Helper for header comparison with skip/disable
    private boolean headersEqual(ConsumerRecord<byte[], byte[]> a, ConsumerRecord<byte[], byte[]> b, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        if (disableHeaderComparison) return true;
        if (a == null || b == null) return false;
        var ha = a.headers();
        var hb = b.headers();
        java.util.Map<String, byte[]> mapA = new java.util.HashMap<>();
        java.util.Map<String, byte[]> mapB = new java.util.HashMap<>();
        for (org.apache.kafka.common.header.Header headerA : ha) {
            if (skipHeaderNames != null && skipHeaderNames.contains(headerA.key())) continue;
            mapA.put(headerA.key(), headerA.value());
        }
        for (org.apache.kafka.common.header.Header headerB : hb) {
            if (skipHeaderNames != null && skipHeaderNames.contains(headerB.key())) continue;
            mapB.put(headerB.key(), headerB.value());
        }
        if (mapA.size() != mapB.size()) return false;
        for (String k : mapA.keySet()) {
            if (!mapB.containsKey(k) || !Arrays.equals(mapA.get(k), mapB.get(k))) return false;
        }
        for (String k : mapB.keySet()) {
            if (!mapA.containsKey(k) || !Arrays.equals(mapB.get(k), mapA.get(k))) return false;
        }
        return true;
    }
}
