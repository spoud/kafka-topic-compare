package io.spoud.kafka.topiccompare;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import java.util.Properties;
import io.spoud.kafka.topiccompare.TopicCompareService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TopicCompareServiceTest {
    static KafkaContainer kafkaA;
    static KafkaContainer kafkaB;

    @BeforeAll
    static void setup() {
        kafkaA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));
        kafkaB = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));
        kafkaA.start();
        kafkaB.start();
    }

    @Test
    void testCompareTopicsWithDifferences() {
        String topicA = "test-topic-a";
        String topicB = "test-topic-b";
        produceTestMessages(kafkaA.getBootstrapServers(), topicA, new int[]{1,2,3,4});
        produceTestMessages(kafkaB.getBootstrapServers(), topicB, new int[]{3,4,5,6});
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "test-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "test-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        // Assert differences: Only in A (1,2), Only in B (5,6)
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        long missingAtEnd = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.MISSING_AT_END).count();
        assert onlyInA == 2 : "Expected 2 ONLY_IN_A, got " + onlyInA;
        assert missingAtEnd == 2 : "Expected 2 MISSING_AT_END, got " + missingAtEnd;
        assert onlyInB == 0 : "Expected 0 ONLY_IN_B, got " + onlyInB;
    }

    private void produceTestMessages(KafkaProducer<byte[], byte[]> producer, String topic, int[] values) {
        for (int v : values) {
            byte[] payload = new byte[]{(byte)v};
            long timestamp = v;
            producer.send(new ProducerRecord<>(topic, null, timestamp, null, payload));
        }
        producer.flush();
    }

    private void produceTestMessages(String bootstrapServers, String topic, int[] values) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            produceTestMessages(producer, topic, values);
        }
    }

    private void produceTestMessage(KafkaProducer<byte[], byte[]> producer, String topic, byte[] key, byte[] value, long timestamp) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, timestamp, key, value);
        producer.send(record);
        producer.flush();
    }

    private void produceTestMessage(String bootstrapServers, String topic, byte[] key, byte[] value, long timestamp) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            produceTestMessage(producer, topic, key, value, timestamp);
        }
    }

    @Test
    void testIdenticalTopics() {
        String topicA = "identical-a";
        String topicB = "identical-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{1,2,3};
        byte[] value = new byte[]{10};
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "identical-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "identical-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        assert logger.getDifferences().isEmpty() : "Expected no differences, got " + logger.getDifferences();
    }

    @Test
    void testExtraMessagesInA() {
        String topicA = "extra-a";
        String topicB = "extra-b";
        // A: 1,2,3,4,5; B: 1,2,4,5 (3 is missing in B)
        produceTestMessages(kafkaA.getBootstrapServers(), topicA, new int[]{1,2,3,4,5});
        produceTestMessages(kafkaB.getBootstrapServers(), topicB, new int[]{1,2,4,5});
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "extra-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "extra-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        assert onlyInA == 1 : "Expected 1 ONLY_IN_A, got " + onlyInA;
    }

    @Test
    void testExtraMessagesInB() {
        String topicA = "extra2-a";
        String topicB = "extra2-b";
        // A: 1,2,4,5; B: 1,2,3,4,5 (3 is missing in A)
        produceTestMessages(kafkaA.getBootstrapServers(), topicA, new int[]{1,2,4,5});
        produceTestMessages(kafkaB.getBootstrapServers(), topicB, new int[]{1,2,3,4,5});
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "extra2-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "extra2-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        assert onlyInB == 1 : "Expected 1 ONLY_IN_B, got " + onlyInB;
    }

    @Test
    void testUniqueMessagesBoth() {
        String topicA = "unique-a";
        String topicB = "unique-b";
        long ts = System.currentTimeMillis();
        // Unique in A
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, new byte[]{1}, new byte[]{10}, ts);
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, new byte[]{3}, new byte[]{11}, ts);
        // Unique in B
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, new byte[]{2}, new byte[]{20}, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, new byte[]{3}, new byte[]{11}, ts);
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "unique-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "unique-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        assert onlyInA == 1 : "Expected 1 ONLY_IN_A, got " + onlyInA;
        assert onlyInB == 1 : "Expected 1 ONLY_IN_B, got " + onlyInB;
    }

    @Test
    void testDuplicateMessagesInA() {
        String topicA = "dup-a";
        String topicB = "dup-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{3};
        byte[] value = new byte[]{30};
        // Produce duplicate to A
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
        // Also produce to B for matching
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "dup-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "dup-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long duplicatesInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_A).count();
        assert duplicatesInA == 1 : "Expected 1 DUPLICATE_IN_A, got " + duplicatesInA;
    }

    @Test
    void testDuplicateMessagesInB() {
        String topicA = "dup2-a";
        String topicB = "dup2-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{4};
        byte[] value = new byte[]{40};
        // Produce duplicate to B
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
        // Also produce to A for matching
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "dup2-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "dup2-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long duplicatesInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_B).count();
        assert duplicatesInB == 1 : "Expected 1 DUPLICATE_IN_B, got " + duplicatesInB;
    }

    @Test
    void testNullKeyHandling() {
        String topicA = "nullkey-a";
        String topicB = "nullkey-b";
        long ts = System.currentTimeMillis();
        byte[] value = new byte[]{50};
        // Null key in both
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, null, value, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, null, value, ts);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "nullkey-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "nullkey-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        assert logger.getDifferences().isEmpty() : "Expected no differences for null key, got " + logger.getDifferences();
    }

    @Test
    void testSameKeyDifferentTimestamp() {
        String topicA = "skt-a";
        String topicB = "skt-b";
        byte[] key = new byte[]{5};
        byte[] key2 = new byte[]{6};
        byte[] value = new byte[]{60};
        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        long ts3 = ts2 + 1000;
        // Same key, different timestamps
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts1);
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key2, value, ts3);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts2);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key2, value, ts3);
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "skt-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "skt-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        assert onlyInA == 1 : "Expected 1 ONLY_IN_A for different timestamps, got " + onlyInA;
        assert onlyInB == 1 : "Expected 1 ONLY_IN_B for different timestamps, got " + onlyInB;
    }

    @Test
    void testEmptyTopics() {
        String topicA = "empty-a";
        String topicB = "empty-b";
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "empty-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "empty-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        assert logger.getDifferences().isEmpty() : "Expected no differences for empty topics, got " + logger.getDifferences();
    }

    @Test
    void testBinaryKey() {
        String topicA = "bin-a";
        String topicB = "bin-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{0, (byte)0xFF, (byte)0x80}; // non-UTF-8
        byte[] value = new byte[]{70};
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "bin-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "bin-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        assert logger.getDifferences().isEmpty() : "Expected no differences for binary key, got " + logger.getDifferences();
    }

    @Test
    void testLargeVolume() {
        String topicA = "large-a";
        String topicB = "large-b";
        long ts = System.currentTimeMillis();
        Properties prodPropsA = new Properties();
        prodPropsA.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaA.getBootstrapServers());
        prodPropsA.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsA.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Properties prodPropsB = new Properties();
        prodPropsB.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaB.getBootstrapServers());
        prodPropsB.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsB.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerA = new KafkaProducer<>(prodPropsA);
             KafkaProducer<byte[], byte[]> producerB = new KafkaProducer<>(prodPropsB)) {
            for (int i = 0; i < 100; i++) {
                byte[] key = new byte[]{(byte)i};
                byte[] value = new byte[]{(byte)(i+100)};
                produceTestMessage(producerA, topicA, key, value, ts);
                produceTestMessage(producerB, topicB, key, value, ts);
            }
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "large-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "large-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 50, logger); // Should only compare 50
        assert logger.getDifferences().isEmpty() : "Expected no differences for first 50 messages, got " + logger.getDifferences();
    }

    @Test
    void testHeaderDifference() {
        String topicA = "header-a";
        String topicB = "header-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{42};
        byte[] value = new byte[]{99};
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "header-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "header-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        // Produce to A with header foo=bar
        produceTestMessageWithHeader(kafkaA.getBootstrapServers(), topicA, key, value, ts, "foo", new byte[]{1});
        // Produce to B with header foo=baz
        produceTestMessageWithHeader(kafkaB.getBootstrapServers(), topicB, key, value, ts, "foo", new byte[]{2});
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long headerDiffs = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.HEADER_DIFFERENCE).count();
        assert headerDiffs == 1 : "Expected 1 HEADER_DIFFERENCE, got " + headerDiffs;
    }

    private void produceTestMessageWithHeader(KafkaProducer<byte[], byte[]> producer, String topic, byte[] key, byte[] value, long timestamp, String headerKey, byte[] headerValue) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, timestamp, key, value);
        record.headers().add(headerKey, headerValue);
        producer.send(record);
        producer.flush();
    }

    private void produceTestMessageWithHeader(String bootstrapServers, String topic, byte[] key, byte[] value, long timestamp, String headerKey, byte[] headerValue) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            produceTestMessageWithHeader(producer, topic, key, value, timestamp, headerKey, headerValue);
        }
    }

    @Test
    void testHighVolumeWithDuplicatesInB() {
        String topicA = "highvol-a";
        String topicB = "highvol-b";
        long ts = System.currentTimeMillis();
        int total = 10_000;
        int duplicateStart = 4950;
        int duplicateCount = 100;
        Properties prodPropsA = new Properties();
        prodPropsA.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaA.getBootstrapServers());
        prodPropsA.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsA.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Properties prodPropsB = new Properties();
        prodPropsB.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaB.getBootstrapServers());
        prodPropsB.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsB.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerA = new KafkaProducer<>(prodPropsA);
             KafkaProducer<byte[], byte[]> producerB = new KafkaProducer<>(prodPropsB)) {
            // Produce 10,000 unique messages to both topics
            for (int i = 0; i < total; i++) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerA, topicA, key, value, ts + i);
                produceTestMessage(producerB, topicB, key, value, ts + i);
            }
            // In B, repeat 100 events (simulate replication restart)
            for (int i = duplicateStart; i < duplicateStart + duplicateCount; i++) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerB, topicB, key, value, ts + i);
            }
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "highvol-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "highvol-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, total + duplicateCount, logger);
        long duplicatesInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_B).count();
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        long duplicatesInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_A).count();
        assert duplicatesInB == duplicateCount : "Expected " + duplicateCount + " DUPLICATE_IN_B, got " + duplicatesInB;
        assert onlyInA == 0 : "Expected 0 ONLY_IN_A, got " + onlyInA;
        assert onlyInB == 0 : "Expected 0 ONLY_IN_B, got " + onlyInB;
        assert duplicatesInA == 0 : "Expected 0 DUPLICATE_IN_A, got " + duplicatesInA;
    }

    @Test
    void testOutOfOrderEvents() {
        String topicA = "outoforder-a";
        String topicB = "outoforder-b";
        long ts = System.currentTimeMillis();
        int total = 10_000;
        int shuffleStart = 4950;
        int shuffleCount = 50;
        Properties prodPropsA = new Properties();
        prodPropsA.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaA.getBootstrapServers());
        prodPropsA.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsA.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Properties prodPropsB = new Properties();
        prodPropsB.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaB.getBootstrapServers());
        prodPropsB.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsB.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerA = new KafkaProducer<>(prodPropsA);
             KafkaProducer<byte[], byte[]> producerB = new KafkaProducer<>(prodPropsB)) {
            // Produce all events in order to topicA
            for (int i = 0; i < total; i++) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerA, topicA, key, value, ts + i);
            }
            // Produce events to topicB: before shuffle range
            for (int i = 0; i < shuffleStart; i++) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerB, topicB, key, value, ts + i);
            }
            // Collect and rotate the middle 50 events (guaranteed all out of order)
            java.util.List<Integer> rotated = new java.util.ArrayList<>();
            for (int i = shuffleStart; i < shuffleStart + shuffleCount; i++) {
                rotated.add(i);
            }
            java.util.Collections.rotate(rotated, 1); // rotate by 1 to ensure all are out of order
            for (int i : rotated) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerB, topicB, key, value, ts + i);
            }
            // Produce remaining events in order
            for (int i = shuffleStart + shuffleCount; i < total; i++) {
                byte[] key = new byte[]{(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
                byte[] value = new byte[]{(byte)(i % 256)};
                produceTestMessage(producerB, topicB, key, value, ts + i);
            }
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "outoforder-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "outoforder-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, total, logger);
        long outOfOrder = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.OUT_OF_ORDER).count();
        assert outOfOrder == shuffleCount : "Expected " + shuffleCount + " OUT_OF_ORDER, got " + outOfOrder;
    }

    @Test
    void testCompareTopicsWithStartTimestamp() {
        String topicA = "timestamp-a";
        String topicB = "timestamp-b";
        long baseTs = System.currentTimeMillis();
        int total = 10;
        Properties prodPropsA = new Properties();
        prodPropsA.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaA.getBootstrapServers());
        prodPropsA.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsA.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Properties prodPropsB = new Properties();
        prodPropsB.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaB.getBootstrapServers());
        prodPropsB.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsB.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerA = new KafkaProducer<>(prodPropsA);
             KafkaProducer<byte[], byte[]> producerB = new KafkaProducer<>(prodPropsB)) {
            for (int i = 0; i < total; i++) {
                byte[] key = new byte[]{(byte)i};
                byte[] value = new byte[]{(byte)(i+100)};
                long ts = baseTs + i * 1000;
                produceTestMessage(producerA, topicA, key, value, ts);
                produceTestMessage(producerB, topicB, key, value, ts);
            }
        }
        // Use a start timestamp that skips the first 5 messages
        long startTs = baseTs + 5 * 1000;
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "timestamp-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "timestamp-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, total, logger, startTs);
        // Only messages with i >= 5 should be compared
        assert logger.getDifferences().isEmpty() : "Expected no differences for messages at/after start timestamp, got " + logger.getDifferences();
    }

    @Test
    void testIgnoreOffsetDifferencesWithStartTimestamp() {
        String topicA = "offsets-a";
        String topicB = "offsets-b";
        long baseTs = System.currentTimeMillis();
        // Produce 20 messages to A before the start timestamp
        for (int i = 0; i < 20; i++) {
            byte[] key = new byte[]{(byte)i};
            byte[] value = new byte[]{(byte)(i+100)};
            produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, baseTs + i * 1000);
        }
        // Produce 10 identical messages to both A and B at/after the start timestamp
        long startTs = baseTs + 20 * 1000;
        for (int i = 20; i < 30; i++) {
            byte[] key = new byte[]{(byte)i};
            byte[] value = new byte[]{(byte)(i+100)};
            produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, baseTs + i * 1000);
            produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, baseTs + i * 1000);
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "offsets-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "offsets-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 20, logger, startTs);
        assert logger.getDifferences().isEmpty() : "Expected no differences when comparing only messages at/after startTimestamp, got " + logger.getDifferences();
    }

    @Test
    void testOffsetReportingWithDifferenceAfterStartTimestamp() {
        String topicA = "offsets-diff-a";
        String topicB = "offsets-diff-b";
        long baseTs = System.currentTimeMillis();
        // Produce 20 messages to A before the start timestamp
        for (int i = 0; i < 20; i++) {
            byte[] key = new byte[]{(byte)i};
            byte[] value = new byte[]{(byte)(i+100)};
            produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, baseTs + i * 1000);
        }
        // Produce 10 messages to both A and B at/after the start timestamp, but make one different in B
        long startTs = baseTs + 20 * 1000;
        int diffIndex = 5; // introduce a difference at this index
        for (int i = 20; i < 30; i++) {
            byte[] key = new byte[]{(byte)i};
            byte[] valueA = new byte[]{(byte)(i+100)};
            byte[] valueB = (i == 20 + diffIndex) ? new byte[]{(byte)99} : new byte[]{(byte)(i+100)};
            if (i != 24)
                produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, valueA, baseTs + i * 1000);
            if (i != 25)
                produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, valueB, baseTs + i * 1000);
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "offsets-diff-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "offsets-diff-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 20, logger, startTs);
        long headerDiffs = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.HEADER_DIFFERENCE).count();
        long onlyInA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).count();
        long onlyInB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).count();
        long missingAtEnd = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.MISSING_AT_END).count();
        long outOfOrder = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.OUT_OF_ORDER).count();
        // There should be exactly one difference (value mismatch, so treated as ONLY_IN_A and ONLY_IN_B)
        long totalDiffs = headerDiffs + onlyInA + onlyInB + missingAtEnd + outOfOrder;
        if (totalDiffs != 2) {
            System.out.println("Differences found:");
            logger.getDifferences().forEach(d -> System.out.println(d));
        }
        assert onlyInA == 1 : "Expected 1 ONLY_IN_A for the differing message, got " + onlyInA;
        assert onlyInB == 1 : "Expected 1 ONLY_IN_B for the differing message, got " + onlyInB;
        // Check that the offsets correspond to the correct positions (should be offset 25 in both topics)
        Difference diffA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_A).findFirst().orElse(null);
        Difference diffB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.ONLY_IN_B).findFirst().orElse(null);
        assert diffA != null && diffB != null;
        long offsetA = diffA.getRecordA() != null ? diffA.getRecordA().offset() : -1;
        long offsetB = diffB.getRecordB() != null ? diffB.getRecordB().offset() : -1;
        assert offsetA >= 0 && offsetB >= 0 : "Offsets should be present in the difference records";
    }

    /**
     * Helper to create a topic with custom properties (e.g., compacted topic).
     */
    private void createTopicWithProperties(String bootstrapServers, String topic, int partitions, short replication, Map<String, String> configs) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, replication).configs(configs);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to create topic: " + topic, e);
        }
    }

    @Test
    void testCompactedTopics() throws Exception {
        String topicA = "compacted-a";
        String topicB = "compacted-b";
        // Create compacted topic on clusterB only
        createTopicWithProperties(
            kafkaB.getBootstrapServers(),
            topicB,
            1,
            (short)1,
            Map.of(
                "cleanup.policy", "compact",
                "segment.ms", "100",
                "min.cleanable.dirty.ratio", "0.01",
                "delete.retention.ms", "100"
            )
        );
        // Give Kafka a moment to create the topic
        Thread.sleep(500);
        // Produce messages with duplicate keys
        // Key 1: value 10, then 20 (should compact to 20)
        // Key 2: value 30 (should remain)
        byte[] key1 = new byte[]{1};
        byte[] key2 = new byte[]{2};
        byte[] key3 = new byte[]{2};
        byte[] value10 = new byte[]{10};
        byte[] value20 = new byte[]{20};
        byte[] value30 = new byte[]{30};
        long ts = System.currentTimeMillis();
        // Both clusters get same final state
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key1, value10, ts);
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key1, value20, ts+1);
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key2, value30, ts+2);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key1, value10, ts);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key1, value20, ts+1);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key2, value30, ts+2);
        // Wait for compaction to run on clusterB
        Thread.sleep(2000);
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "compacted-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "compacted-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        assert logger.getDifferences().isEmpty() : "Expected no differences after compaction, got " + logger.getDifferences();

        // Now, produce a different value for key1 in clusterA only
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key1, new byte[]{99}, ts+3);
        // add a message on bosth so it's not marked as missing on the end of topic
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key3, new byte[]{99}, ts+3);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key3, new byte[]{99}, ts+3);

        Thread.sleep(500);
        logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        boolean found = logger.getDifferences().stream().anyMatch(d -> d.getType() == Difference.Type.ONLY_IN_A);
        assert found : "Expected a difference for key1 (ONLY_IN_A) after diverging value, got " + logger.getDifferences();
    }

    @Test
    void testSameKeyDifferentTimestampsNotDuplicate() {
        String topicA = "timestamp-key-a";
        String topicB = "timestamp-key-b";
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{42};
        byte[] value = new byte[]{99};
        // Produce two records with same key/value but different timestamps to topicA
        Properties prodPropsA = new Properties();
        prodPropsA.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaA.getBootstrapServers());
        prodPropsA.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsA.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerA = new KafkaProducer<>(prodPropsA)) {
            produceTestMessage(producerA, topicA, key, value, ts);
            produceTestMessage(producerA, topicA, key, value, ts + 1000);
        }
        // Produce only one record to topicB
        Properties prodPropsB = new Properties();
        prodPropsB.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaB.getBootstrapServers());
        prodPropsB.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodPropsB.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producerB = new KafkaProducer<>(prodPropsB)) {
            produceTestMessage(producerB, topicB, key, value, ts);
        }
        Properties propsA = new Properties();
        propsA.put("bootstrap.servers", kafkaA.getBootstrapServers());
        propsA.put("group.id", "timestamp-key-a");
        propsA.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsA.put("auto.offset.reset", "earliest");
        Properties propsB = new Properties();
        propsB.put("bootstrap.servers", kafkaB.getBootstrapServers());
        propsB.put("group.id", "timestamp-key-b");
        propsB.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propsB.put("auto.offset.reset", "earliest");
        CollectingDifferenceLogger logger = new CollectingDifferenceLogger();
        new TopicCompareService().compareTopics(propsA, topicA, propsB, topicB, 10, logger);
        long duplicatesA = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_A).count();
        long duplicatesB = logger.getDifferences().stream().filter(d -> d.getType() == Difference.Type.DUPLICATE_IN_B).count();
        assert duplicatesA == 0 : "Should not report duplicates in A for same key with different timestamps, got " + duplicatesA;
        assert duplicatesB == 0 : "Should not report duplicates in B for same key with different timestamps, got " + duplicatesB;
    }
}
