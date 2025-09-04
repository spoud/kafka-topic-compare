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
        assert onlyInA == 2 : "Expected 2 ONLY_IN_A, got " + onlyInA;
        assert onlyInB == 2 : "Expected 2 ONLY_IN_B, got " + onlyInB;
    }

    private void produceTestMessages(String bootstrapServers, String topic, int[] values) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int v : values) {
                byte[] payload = new byte[]{(byte)v};
                // Use null as key, value as payload (original intent)
                producer.send(new ProducerRecord<>(topic, null, payload));
            }
            producer.flush();
        }
    }

    private void produceTestMessage(String bootstrapServers, String topic, byte[] key, byte[] value, long timestamp) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, timestamp, key, value);
            producer.send(record);
            producer.flush();
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
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{1};
        byte[] value = new byte[]{10};
        // Only produce to A
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
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
        long ts = System.currentTimeMillis();
        byte[] key = new byte[]{2};
        byte[] value = new byte[]{20};
        // Only produce to B
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
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
        // Unique in B
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, new byte[]{2}, new byte[]{20}, ts);
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
        // TODO: Assert correct handling of null key
    }

    @Test
    void testSameKeyDifferentTimestamp() {
        String topicA = "skt-a";
        String topicB = "skt-b";
        byte[] key = new byte[]{5};
        byte[] value = new byte[]{60};
        long ts1 = System.currentTimeMillis();
        long ts2 = ts1 + 1000;
        // Same key, different timestamps
        produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts1);
        produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts2);
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
        // TODO: Assert treated as different
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
        // TODO: Assert no differences
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
        // TODO: Assert correct handling of binary key
    }

    @Test
    void testLargeVolume() {
        String topicA = "large-a";
        String topicB = "large-b";
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            byte[] key = new byte[]{(byte)i};
            byte[] value = new byte[]{(byte)(i+100)};
            produceTestMessage(kafkaA.getBootstrapServers(), topicA, key, value, ts);
            produceTestMessage(kafkaB.getBootstrapServers(), topicB, key, value, ts);
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
        // TODO: Assert maxMessages limit
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

    private void produceTestMessageWithHeader(String bootstrapServers, String topic, byte[] key, byte[] value, long timestamp, String headerKey, byte[] headerValue) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, timestamp, key, value);
            record.headers().add(headerKey, headerValue);
            producer.send(record);
            producer.flush();
        }
    }
}
