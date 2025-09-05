package io.spoud.kafka.topiccompare;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Difference {
    public enum Type {
        ONLY_IN_A,
        ONLY_IN_B,
        DUPLICATE_IN_A,
        DUPLICATE_IN_B,
        HEADER_DIFFERENCE,
        OUT_OF_ORDER
    }

    private final Type type;
    private final ConsumerRecord<byte[], byte[]> recordA;
    private final ConsumerRecord<byte[], byte[]> recordB;
    private final String keyHash;

    public Difference(Type type, ConsumerRecord<byte[], byte[]> recordA, ConsumerRecord<byte[], byte[]> recordB, String keyHash) {
        this.type = type;
        this.recordA = recordA;
        this.recordB = recordB;
        this.keyHash = keyHash;
    }

    public Type getType() { return type; }
    public ConsumerRecord<byte[], byte[]> getRecordA() { return recordA; }
    public ConsumerRecord<byte[], byte[]> getRecordB() { return recordB; }
    public String getKeyHash() { return keyHash; }
}
