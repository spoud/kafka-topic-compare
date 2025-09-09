package io.spoud.kafka.topiccompare;

import org.junit.jupiter.api.Test;

public class TopicCompareTest {
    @Test
    void testMainWithNoArgs() {
        try {
            new TopicCompare().run(new String[]{});
        } catch (Exception e) {
            assert false : "Main should not throw with no args: " + e.getMessage();
        }
    }

    @Test
    void testMainWithHelpArg() {
        try {
            new TopicCompare().run(new String[]{"--help"});
        } catch (Exception e) {
            assert false : "Main should not throw with --help: " + e.getMessage();
        }
    }

    // Add more CLI argument tests as needed
}

