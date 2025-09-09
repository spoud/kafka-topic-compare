package io.spoud.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SourceMetaToHeadersTest {
    private static final String CLUSTER = "test-cluster";
    private static final String OTHER_CLUSTER = "other-cluster";

    private SourceMetaToHeaders<SourceRecord> createTransform(boolean dropIfPresent) {
        Map<String, Object> config = new HashMap<>();
        config.put(SourceMetaToHeaders.PROVENANCE_CLUSTER_NAME, CLUSTER);
        config.put(SourceMetaToHeaders.PROVENANCE_DROP_IF_PRESENT, dropIfPresent);
        SourceMetaToHeaders<SourceRecord> t = new SourceMetaToHeaders<>();
        t.configure(config);
        return t;
    }

    private SourceRecord recordWithProvenance(String provenance) {
        SourceRecord r = new SourceRecord(
                Collections.singletonMap("cluster", "foo"),
                Collections.singletonMap("offset", 1),
                "topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "value");
        if (provenance != null) {
            r.headers().add(SourceMetaToHeaders.PROVENANCE_HEADER, provenance.getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA);
        }
        return r;
    }

    @Test
    void addsProvenanceHeaderIfMissing() {
        SourceMetaToHeaders<SourceRecord> t = createTransform(false);
        SourceRecord r = recordWithProvenance(null);
        SourceRecord out = t.apply(r);
        assertNotNull(out);
        byte[] header = (byte[]) out.headers().lastWithName(SourceMetaToHeaders.PROVENANCE_HEADER).value();
        assertEquals(CLUSTER, new String(header, StandardCharsets.UTF_8));
    }

    @Test
    void appendsClusterIfNotPresent() {
        SourceMetaToHeaders<SourceRecord> t = createTransform(false);
        SourceRecord r = recordWithProvenance(OTHER_CLUSTER);
        SourceRecord out = t.apply(r);
        assertNotNull(out);
        byte[] header = (byte[]) out.headers().lastWithName(SourceMetaToHeaders.PROVENANCE_HEADER).value();
        assertEquals(OTHER_CLUSTER + "," + CLUSTER, new String(header, StandardCharsets.UTF_8));
    }

    @Test
    void doesNotAddClusterAgainIfPresentAndDropDisabled() {
        SourceMetaToHeaders<SourceRecord> t = createTransform(false);
        SourceRecord r = recordWithProvenance(CLUSTER);
        SourceRecord out = t.apply(r);
        assertNotNull(out);
        byte[] header = (byte[]) out.headers().lastWithName(SourceMetaToHeaders.PROVENANCE_HEADER).value();
        assertEquals(CLUSTER, new String(header, StandardCharsets.UTF_8));
    }

    @Test
    void dropsRecordIfClusterPresentAndDropEnabled() {
        SourceMetaToHeaders<SourceRecord> t = createTransform(true);
        SourceRecord r = recordWithProvenance(CLUSTER);
        SourceRecord out = t.apply(r);
        assertNull(out);
    }
}

