package io.spoud.kafka.topiccompare;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Difference {
    public enum Type {
        ONLY_IN_A,
        ONLY_IN_B,
        DUPLICATE_IN_A,
        DUPLICATE_IN_B,
        HEADER_DIFFERENCE,
        OUT_OF_ORDER,
        MISSING_AT_END, // messages only at the end
        DIFFERENT_PARTITION // messages with same key but different partition
    }

    private final Type type;
    private final ConsumerRecord<byte[], byte[]> recordA;
    private final ConsumerRecord<byte[], byte[]> recordB;
    private final ConsumerRecord<byte[], byte[]> duplicateRecord;
    private final String keyHash;

    public Difference(Type type, ConsumerRecord<byte[], byte[]> recordA, ConsumerRecord<byte[], byte[]> recordB, String keyHash) {
        this(type, recordA, recordB, keyHash, null);
    }

    public Difference(Type type, ConsumerRecord<byte[], byte[]> recordA, ConsumerRecord<byte[], byte[]> recordB, String keyHash, ConsumerRecord<byte[], byte[]> duplicateRecord) {
        this.type = type;
        this.recordA = recordA;
        this.recordB = recordB;
        this.keyHash = keyHash;
        this.duplicateRecord = duplicateRecord;
    }

    public Type getType() { return type; }
    public ConsumerRecord<byte[], byte[]> getRecordA() { return recordA; }
    public ConsumerRecord<byte[], byte[]> getRecordB() { return recordB; }
    public ConsumerRecord<byte[], byte[]> getDuplicateRecord() { return duplicateRecord; }
    public String getKeyHash() { return keyHash; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: ").append(type);
        sb.append(", KeyHash: ").append(keyHash);
        if (recordA != null) {
            sb.append(", A: value=").append(recordA.value() != null ? java.util.Arrays.toString(recordA.value()) : "null");
        }
        if (recordB != null) {
            sb.append(", B: value=").append(recordB.value() != null ? java.util.Arrays.toString(recordB.value()) : "null");
        }
        if (duplicateRecord != null) {
            sb.append(", Duplicate: value=").append(duplicateRecord.value() != null ? java.util.Arrays.toString(duplicateRecord.value()) : "null");
        }
        return sb.toString();
    }
}
