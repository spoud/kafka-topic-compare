package io.spoud.kafka.topiccompare;

public class CompareResult {
    public final CompareStats statsA;
    public final CompareStats statsB;

    public CompareResult(CompareStats statsA, CompareStats statsB) {
        this.statsA = statsA;
        this.statsB = statsB;
    }
}

