package com.flinkstress.harness.source;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.model.Event;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class RecordFactoryTest {

    private static RecordFactory factory(Map<String, String> overrides, long seed) {
        return new RecordFactory(HarnessConfig.fromMap(overrides), new Random(seed));
    }

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void producesPayloadOfConfiguredSizeAndEmitTime() {
        RecordFactory f = factory(map("source.recordSizeBytes", "512"), 1);
        long now = 1_000_000L;
        Event e = f.create(42L, now);
        assertThat(e.sizeBytes()).isEqualTo(512);
        assertThat(e.seqNo).isEqualTo(42L);
        assertThat(e.emitTimeMs).isEqualTo(now);
        assertThat(e.eventTimeMs).isEqualTo(now); // late data disabled
        assertThat(e.key).startsWith("k");
    }

    @Test
    void uniformKeysStayWithinKeyspace() {
        RecordFactory f = factory(map("source.numKeys", "50"), 7);
        for (int i = 0; i < 5000; i++) {
            String key = f.pickKey();
            int idx = Integer.parseInt(key.substring(1));
            assertThat(idx).isBetween(0, 49);
        }
    }

    @Test
    void hotKeyShareIsApproximatelyHonoured() {
        RecordFactory f = factory(map(
                "skew.enabled", "true",
                "source.numKeys", "1000",
                "skew.hotKeyCount", "10",
                "skew.hotKeyTrafficPct", "0.8"), 123);

        int hot = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            int idx = Integer.parseInt(f.pickKey().substring(1));
            if (idx < 10) {
                hot++;
            }
        }
        double hotFraction = (double) hot / total;
        assertThat(hotFraction).isBetween(0.78, 0.82); // ~0.8 within tolerance
    }

    @Test
    void lateDataFractionAndRangeAreHonoured() {
        RecordFactory f = factory(map(
                "late.enabled", "true",
                "late.pct", "0.2",
                "late.minMs", "1000",
                "late.maxMs", "5000"), 99);

        long now = 10_000_000L;
        int late = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            long et = f.pickEventTime(now);
            if (et < now) {
                late++;
                long lateness = now - et;
                assertThat(lateness).isBetween(1000L, 5000L);
            } else {
                assertThat(et).isEqualTo(now);
            }
        }
        double lateFraction = (double) late / total;
        assertThat(lateFraction).isBetween(0.18, 0.22);
    }

    @Test
    void noLatenessWhenDisabled() {
        RecordFactory f = factory(map("late.enabled", "false"), 3);
        long now = 500L;
        for (int i = 0; i < 1000; i++) {
            assertThat(f.pickEventTime(now)).isEqualTo(now);
        }
    }

    @Test
    void sameSeedProducesIdenticalStream() {
        Map<String, String> cfg = map("skew.enabled", "true", "late.enabled", "true");
        RecordFactory a = factory(cfg, 555);
        RecordFactory b = factory(cfg, 555);
        for (int i = 0; i < 1000; i++) {
            assertThat(a.create(i, 1234L)).isEqualTo(b.create(i, 1234L));
        }
    }
}
