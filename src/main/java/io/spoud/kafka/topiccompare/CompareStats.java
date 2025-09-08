package io.spoud.kafka.topiccompare;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

public class CompareStats {
    public Map<TopicPartition, Long> startOffsets;
    public Map<TopicPartition, Long> endOffsets;
    public boolean endReached;
    public int eventsRead;
    public boolean timestampsMonotonous = true;

    public CompareStats(Map<TopicPartition, Long> startOffsets) {
        this.startOffsets = startOffsets;
    }
}

