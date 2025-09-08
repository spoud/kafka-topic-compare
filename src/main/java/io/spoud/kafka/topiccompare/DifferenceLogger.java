package io.spoud.kafka.topiccompare;

public interface DifferenceLogger {
    void log(Difference difference);
}
