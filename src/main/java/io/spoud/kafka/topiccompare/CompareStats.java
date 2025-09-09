package io.spoud.kafka.topiccompare;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

public class CompareStats {
    public Map<TopicPartition, Long> startOffsets;
    public Map<TopicPartition, Long> endOffsets;
    public Map<TopicPartition, Boolean> endReached = new java.util.HashMap<>();
    public Map<TopicPartition, Integer> eventsRead = new java.util.HashMap<>();
    public Map<TopicPartition, Boolean> timestampsMonotonous = new java.util.HashMap<>();

    public CompareStats(Map<TopicPartition, Long> startOffsets) {
        this.startOffsets = startOffsets;
        // Initialize per-partition maps
        for (TopicPartition tp : startOffsets.keySet()) {
            endReached.put(tp, false);
            eventsRead.put(tp, 0);
            timestampsMonotonous.put(tp, true);
        }
    }
}
