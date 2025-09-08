package io.spoud.kafka.topiccompare;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectingDifferenceLogger implements DifferenceLogger {
    private final List<Difference> differences = new ArrayList<>();

    @Override
    public void log(Difference difference) {
        differences.add(difference);
    }

    public List<Difference> getDifferences() {
        return Collections.unmodifiableList(differences);
    }
}
