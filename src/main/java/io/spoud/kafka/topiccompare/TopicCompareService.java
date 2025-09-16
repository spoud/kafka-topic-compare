package io.spoud.kafka.topiccompare;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TopicCompareService {
    // Unified compareTopics implementation
    private CompareResult compareTopicsImpl(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        // Helper to create a unique string from key only (for compacted) or key+value (for normal)
        BiFunction<byte[], byte[], String> keyHash = (keyBytes, valueBytes) -> {
            if (keyBytes != null) {
                return Base64.getEncoder().encodeToString(keyBytes);
            } else if (valueBytes != null) {
                return "null:" + Arrays.hashCode(valueBytes);
            } else {
                return "null:null";
            }
        };
        // Helper to create a unique string from key, value, and timestamp for duplicate detection
        Function<ConsumerRecord<byte[], byte[]>, String> duplicateKey = record -> {
            return Base64.getEncoder().encodeToString(record.key() == null ? new byte[0] : record.key()) +
                    ":" + Base64.getEncoder().encodeToString(record.value() == null ? new byte[0] : record.value()) +
                    ":" + record.timestamp();
        };
        // Always initialize statsA and statsB with empty maps to avoid NullPointerException
        CompareStats statsA = new CompareStats(new HashMap<>());
        CompareStats statsB = new CompareStats(new HashMap<>());
        try (KafkaConsumer<byte[], byte[]> consumerA = new KafkaConsumer<>(propsA);
             KafkaConsumer<byte[], byte[]> consumerB = new KafkaConsumer<>(propsB)) {
            // --- Refactored per-partition message reading for topic A ---
            Map<TopicPartition, Long> lastOffsetsA = new HashMap<>();
            Map<TopicPartition, Long> effectiveStartOffsetsA = new HashMap<>();
            Map<String, ConsumerRecord<byte[], byte[]>> recordsA = new HashMap<>();
            Set<String> seenA = new HashSet<>();
            List<String> orderA = new ArrayList<>();
            // Track partitions for each key in A
            Map<String, Set<Integer>> keyPartitionsA = new HashMap<>();
            TopicPartition[] partitionsA = consumerA.partitionsFor(topicA).stream()
                    .map(p -> new TopicPartition(topicA, p.partition()))
                    .toArray(TopicPartition[]::new);
            for (TopicPartition partition : partitionsA) {
                consumerA.assign(Collections.singletonList(partition));
                long startOffset;
                if (startTimestamp != null) {
                    Map<TopicPartition, Long> timestampMap = Map.of(partition, startTimestamp);
                    var offsets = consumerA.offsetsForTimes(timestampMap);
                    var offsetAndTimestamp = offsets.get(partition);
                    if (offsetAndTimestamp != null) {
                        startOffset = offsetAndTimestamp.offset();
                        consumerA.seek(partition, startOffset);
                    } else {
                        startOffset = consumerA.beginningOffsets(Collections.singleton(partition)).get(partition);
                        consumerA.seek(partition, startOffset);
                    }
                } else {
                    startOffset = consumerA.beginningOffsets(Collections.singleton(partition)).get(partition);
                    consumerA.seek(partition, startOffset);
                }
                effectiveStartOffsetsA.put(partition, startOffset);
                long lastTimestamp = -1;
                boolean monotonicTimestamps = true;
                int count = 0;
                boolean reachedEnd = false;
                while (count < maxMessages) {
                    var records = consumerA.poll(Duration.ofSeconds(1));
                    if (records.isEmpty()) {
                        reachedEnd = true;
                        break;
                    }
                    for (var record : records) {
                        if (record.partition() != partition.partition()) continue;
                        String key = keyHash.apply(record.key(), compacted ? null : record.value());
                        // Track partitions for this key
                        keyPartitionsA.computeIfAbsent(key, k -> new HashSet<>()).add(partition.partition());
                        if (lastTimestamp > record.timestamp()) {
                            monotonicTimestamps = false;
                        }
                        lastTimestamp = record.timestamp();
                        lastOffsetsA.put(partition, record.offset());
                        if (compacted) {
                            recordsA.put(key, record);
                            if (!seenA.contains(key)) orderA.add(key);
                            seenA.add(key);
                        } else {
                            String dupKey = duplicateKey.apply(record);
                            if (seenA.contains(dupKey)) {
                                logger.log(new Difference(Difference.Type.DUPLICATE_IN_A, record, null, key, recordsA.get(dupKey)));
                            } else {
                                seenA.add(dupKey);
                                recordsA.put(dupKey, record);
                                orderA.add(dupKey);
                            }
                        }
                        count++;
                        if (count >= maxMessages) break;
                    }
                }
                statsA.endReached.put(partition, reachedEnd);
                statsA.eventsRead.put(partition, count);
                statsA.timestampsMonotonous.put(partition, monotonicTimestamps);
            }
            statsA.endOffsets = new HashMap<>(lastOffsetsA);
            statsA.startOffsets = new HashMap<>(effectiveStartOffsetsA);

            // --- Refactored per-partition message reading for topic B ---
            Map<TopicPartition, Long> lastOffsetsB = new HashMap<>();
            Map<TopicPartition, Long> effectiveStartOffsetsB = new HashMap<>();
            Map<String, ConsumerRecord<byte[], byte[]>> recordsB = new HashMap<>();
            Set<String> seenB = new HashSet<>();
            List<String> orderB = new ArrayList<>();
            TopicPartition[] partitionsB = consumerB.partitionsFor(topicB).stream()
                    .map(p -> new TopicPartition(topicB, p.partition()))
                    .toArray(TopicPartition[]::new);
            for (TopicPartition partition : partitionsB) {
                consumerB.assign(Collections.singletonList(partition));
                long startOffset;
                if (startTimestamp != null) {
                    Map<TopicPartition, Long> timestampMap = Map.of(partition, startTimestamp);
                    var offsets = consumerB.offsetsForTimes(timestampMap);
                    var offsetAndTimestamp = offsets.get(partition);
                    if (offsetAndTimestamp != null) {
                        startOffset = offsetAndTimestamp.offset();
                        consumerB.seek(partition, startOffset);
                    } else {
                        startOffset = consumerB.beginningOffsets(Collections.singleton(partition)).get(partition);
                        consumerB.seek(partition, startOffset);
                    }
                } else {
                    startOffset = consumerB.beginningOffsets(Collections.singleton(partition)).get(partition);
                    consumerB.seek(partition, startOffset);
                }
                effectiveStartOffsetsB.put(partition, startOffset);
                long lastTimestamp = -1;
                boolean monotonicTimestamps = true;
                int count = 0;
                boolean reachedEnd = false;
                while (count < maxMessages) {
                    var records = consumerB.poll(Duration.ofSeconds(1));
                    if (records.isEmpty()) {
                        reachedEnd = true;
                        break;
                    }
                    for (var record : records) {
                        if (record.partition() != partition.partition()) continue;
                        String key = keyHash.apply(record.key(), compacted ? null : record.value());
                        if (lastTimestamp > record.timestamp()) {
                            monotonicTimestamps = false;
                        }
                        lastTimestamp = record.timestamp();
                        lastOffsetsB.put(partition, record.offset());
                        if (compacted) {
                            recordsB.put(key, record);
                            if (!seenB.contains(key)) orderB.add(key);
                            seenB.add(key);
                        } else {
                            String dupKey = duplicateKey.apply(record);
                            if (seenB.contains(dupKey)) {
                                logger.log(new Difference(Difference.Type.DUPLICATE_IN_B, null, record, key, recordsB.get(dupKey)));
                            } else {
                                seenB.add(dupKey);
                                recordsB.put(dupKey, record);
                                orderB.add(dupKey);
                            }
                        }
                        count++;
                        if (count >= maxMessages) break;
                    }
                }
                statsB.endReached.put(partition, reachedEnd);
                statsB.eventsRead.put(partition, count);
                statsB.timestampsMonotonous.put(partition, monotonicTimestamps);
            }
            statsB.endOffsets = new HashMap<>(lastOffsetsB);
            statsB.startOffsets = new HashMap<>(effectiveStartOffsetsB);

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
                    // Check for different partition
                    if (recA.partition() != recB.partition()) {
                        logger.log(new Difference(Difference.Type.DIFFERENT_PARTITION, recA, recB, key));
                        continue;
                    }
                    // For compacted topics, compare values as well
                    if (compacted) {
                        if (!Arrays.equals(recA.value(), recB.value())) {
                            logger.log(new Difference(Difference.Type.ONLY_IN_A, recA, null, key));
                            logger.log(new Difference(Difference.Type.ONLY_IN_B, null, recB, key));
                        }
                    } else if (!headersEqual(recA, recB, skipHeaderNames, disableHeaderComparison)) {
                        if (!disableHeaderComparison) {
                            logger.log(new Difference(Difference.Type.HEADER_DIFFERENCE, recA, recB, key));
                        }
                    }
                } else if (inA && !inB) {
                    if (!compacted && missingAtEndA.contains(key)) {
                        logger.log(new Difference(Difference.Type.MISSING_AT_END, recordsA.get(key), null, key));
                    } else {
                        logger.log(new Difference(Difference.Type.ONLY_IN_A, recordsA.get(key), null, key));
                    }
                } else if (!inA && inB) {
                    if (!compacted && missingAtEndB.contains(key)) {
                        logger.log(new Difference(Difference.Type.MISSING_AT_END, null, recordsB.get(key), key));
                    } else {
                        logger.log(new Difference(Difference.Type.ONLY_IN_B, null, recordsB.get(key), key));
                    }
                }
            }

            if (!compacted) {
                // Out-of-order detection: compare order of keys as they appear in both topics
                // Check A vs B
                int lastIdxB = -1;
                for (String key : orderA) {
                    if (recordsB.containsKey(key)) {
                        int idxB = orderB.indexOf(key);
                        if (idxB < lastIdxB) {
                            logger.log(new Difference(Difference.Type.OUT_OF_ORDER, recordsA.get(key), recordsB.get(key), key));
                        } else {
                            lastIdxB = idxB;
                        }
                    }
                }
                // Check B vs A (to catch all out-of-order cases)
                int lastIdxA = -1;
                for (String key : orderB) {
                    if (recordsA.containsKey(key)) {
                        int idxA = orderA.indexOf(key);
                        if (idxA < lastIdxA) {
                            logger.log(new Difference(Difference.Type.OUT_OF_ORDER, recordsA.get(key), recordsB.get(key), key));
                        } else {
                            lastIdxA = idxA;
                        }
                    }
                }
            }
        }
        return new CompareResult(statsA, statsB);
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
    public CompareResult compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp) {
        boolean compacted = isCompactedAndLogConfigDiff(propsA, topicA, propsB, topicB);
        return compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, null, false);
    }

    public CompareResult compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted) {
        return compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, null, false);
    }

    public CompareResult compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        boolean compacted = isCompactedAndLogConfigDiff(propsA, topicA, propsB, topicB);
        return compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, skipHeaderNames, disableHeaderComparison);
    }

    public CompareResult compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger, Long startTimestamp, boolean compacted, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        return compareTopicsImpl(propsA, topicA, propsB, topicB, maxMessages, logger, startTimestamp, compacted, skipHeaderNames, disableHeaderComparison);
    }

    // Helper for header comparison with skip/disable
    private boolean headersEqual(ConsumerRecord<byte[], byte[]> a, ConsumerRecord<byte[], byte[]> b, Set<String> skipHeaderNames, boolean disableHeaderComparison) {
        if (disableHeaderComparison) return true;
        if (a == null || b == null) return false;
        var ha = a.headers();
        var hb = b.headers();
        Map<String, byte[]> mapA = new HashMap<>();
        Map<String, byte[]> mapB = new HashMap<>();
        for (Header headerA : ha) {
            if (skipHeaderNames != null && skipHeaderNames.contains(headerA.key())) continue;
            mapA.put(headerA.key(), headerA.value());
        }
        for (Header headerB : hb) {
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
