package com.flinkstress.harness.source;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.model.Event;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * Builds synthetic {@link Event}s honouring the configured record size,
 * key-skew model (hot-key percentage) and late-data injection. All randomness
 * comes from an injected {@link Random} so distributions are reproducible and
 * unit testable.
 */
public final class RecordFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final byte FILL = (byte) 'x';

    private final HarnessConfig cfg;
    private final Random rnd;

    public RecordFactory(HarnessConfig cfg, Random rnd) {
        this.cfg = cfg;
        this.rnd = rnd;
    }

    public Event create(long seqNo, long nowMs) {
        String key = pickKey();
        long eventTime = pickEventTime(nowMs);
        byte[] payload = new byte[Math.max(0, cfg.recordSizeBytes)];
        Arrays.fill(payload, FILL);
        return new Event(seqNo, key, nowMs, eventTime, payload);
    }

    /** Chooses a key index honouring the hot-key skew model, returns "k&lt;idx&gt;". */
    String pickKey() {
        long idx;
        if (cfg.skewEnabled) {
            long cold = cfg.numKeys - cfg.hotKeyCount;
            if (cold <= 0 || rnd.nextDouble() < cfg.hotKeyTrafficPct) {
                idx = rnd.nextInt(cfg.hotKeyCount);
            } else {
                idx = cfg.hotKeyCount + (long) (rnd.nextDouble() * cold);
            }
        } else {
            idx = (long) (rnd.nextDouble() * cfg.numKeys);
        }
        return "k" + idx;
    }

    /** Returns now, or a past timestamp if this record is selected as "late". */
    long pickEventTime(long nowMs) {
        if (cfg.lateEnabled && rnd.nextDouble() < cfg.latePct) {
            long span = cfg.lateMaxMs - cfg.lateMinMs;
            long lateness = cfg.lateMinMs + (span <= 0 ? 0L : (long) (rnd.nextDouble() * (span + 1)));
            return nowMs - lateness;
        }
        return nowMs;
    }
}
