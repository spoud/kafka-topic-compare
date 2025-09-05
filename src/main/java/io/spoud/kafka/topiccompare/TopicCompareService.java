package io.spoud.kafka.topiccompare;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.*;
import java.time.Duration;

public class TopicCompareService {
    public void compareTopics(Properties propsA, String topicA, Properties propsB, String topicB, int maxMessages, DifferenceLogger logger) {
        try (KafkaConsumer<byte[], byte[]> consumerA = new KafkaConsumer<>(propsA);
             KafkaConsumer<byte[], byte[]> consumerB = new KafkaConsumer<>(propsB)) {

            consumerA.subscribe(Collections.singletonList(topicA));
            consumerB.subscribe(Collections.singletonList(topicB));

            Map<String, ConsumerRecord<byte[], byte[]>> recordsA = new HashMap<>();
            Map<String, ConsumerRecord<byte[], byte[]>> recordsB = new HashMap<>();

            Set<String> seenA = new HashSet<>();
            Set<String> seenB = new HashSet<>();
            List<String> orderA = new ArrayList<>();
            List<String> orderB = new ArrayList<>();
            // Helper to create a unique string from key and value
            java.util.function.BiFunction<byte[], byte[], String> keyHash = (keyBytes, valueBytes) -> {
                if (keyBytes != null) {
                    return java.util.Base64.getEncoder().encodeToString(keyBytes);
                } else if (valueBytes != null) {
                    return "null:" + java.util.Arrays.hashCode(valueBytes);
                } else {
                    return "null:null";
                }
            };
            // Read messages from topic A
            int countA = 0;
            while (countA < maxMessages) {
                var records = consumerA.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) break;
                for (var record : records) {
                    String key = keyHash.apply(record.key(), record.value());
                    if (seenA.contains(key)) {
                        logger.log(new Difference(Difference.Type.DUPLICATE_IN_A, record, null, key));
                    } else {
                        seenA.add(key);
                        recordsA.put(key, record);
                        orderA.add(key);
                    }
                    countA++;
                    if (countA >= maxMessages) break;
                }
            }

            // Read messages from topic B
            int countB = 0;
            while (countB < maxMessages) {
                var records = consumerB.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) break;
                for (var record : records) {
                    String key = keyHash.apply(record.key(), record.value());
                    if (seenB.contains(key)) {
                        logger.log(new Difference(Difference.Type.DUPLICATE_IN_B, null, record, key));
                    } else {
                        seenB.add(key);
                        recordsB.put(key, record);
                        orderB.add(key);
                    }
                    countB++;
                    if (countB >= maxMessages) break;
                }
            }

            // Compare
            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(recordsA.keySet());
            allKeys.addAll(recordsB.keySet());

            for (String key : allKeys) {
                boolean inA = recordsA.containsKey(key);
                boolean inB = recordsB.containsKey(key);
                if (inA && inB) {
                    ConsumerRecord<byte[], byte[]> recA = recordsA.get(key);
                    ConsumerRecord<byte[], byte[]> recB = recordsB.get(key);
                    if (!headersEqual(recA, recB)) {
                        logger.log(new Difference(Difference.Type.HEADER_DIFFERENCE, recA, recB, key));
                    }
                } else if (inA && !inB) {
                    logger.log(new Difference(Difference.Type.ONLY_IN_A, recordsA.get(key), null, key));
                } else if (!inA && inB) {
                    logger.log(new Difference(Difference.Type.ONLY_IN_B, null, recordsB.get(key), key));
                }
            }

            // Out-of-order detection
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

    private boolean headersEqual(ConsumerRecord<byte[], byte[]> a, ConsumerRecord<byte[], byte[]> b) {
        if (a == null || b == null) return false;
        var ha = a.headers();
        var hb = b.headers();
        if (ha.toArray().length != hb.toArray().length) return false;
        for (org.apache.kafka.common.header.Header headerA : ha) {
            org.apache.kafka.common.header.Header headerB = hb.lastHeader(headerA.key());
            if (headerB == null || !Arrays.equals(headerA.value(), headerB.value())) return false;
        }
        for (org.apache.kafka.common.header.Header headerB : hb) {
            org.apache.kafka.common.header.Header headerA = ha.lastHeader(headerB.key());
            if (headerA == null || !Arrays.equals(headerB.value(), headerA.value())) return false;
        }
        return true;
    }
}
