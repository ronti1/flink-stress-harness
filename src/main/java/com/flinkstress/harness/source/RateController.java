package com.flinkstress.harness.source;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.RateMode;

import java.io.Serializable;

/**
 * Pure function from elapsed run time to a target job-wide emission rate
 * (records/sec). Kept side-effect free so it can be unit tested directly; the
 * source applies per-subtask scaling and pacing around it.
 */
public final class RateController implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sentinel returned for {@link RateMode#MAX} meaning "do not pace". */
    public static final long UNPACED = Long.MAX_VALUE;

    private final HarnessConfig cfg;

    public RateController(HarnessConfig cfg) {
        this.cfg = cfg;
    }

    public boolean isPaced() {
        return cfg.rateMode != RateMode.MAX;
    }

    /** Job-wide target rate (records/sec) at the given elapsed time. */
    public long currentRatePerSec(long elapsedMs) {
        switch (cfg.rateMode) {
            case CONSTANT:
                return Math.max(0L, cfg.ratePerSec);
            case MAX:
                return UNPACED;
            case RAMP: {
                long stepMs = cfg.rampStepSeconds * 1000L;
                long steps = stepMs > 0 ? (elapsedMs / stepMs) : 0L;
                long rate = cfg.rampStartRatePerSec + steps * cfg.rampStepRatePerSec;
                if (cfg.rampMaxRatePerSec > 0) {
                    rate = Math.min(rate, cfg.rampMaxRatePerSec);
                }
                return Math.max(0L, rate);
            }
            case BURST: {
                long periodMs = cfg.burstPeriodSeconds * 1000L;
                long phase = periodMs > 0 ? (elapsedMs % periodMs) : 0L;
                long peakMs = cfg.burstPeakSeconds * 1000L;
                return Math.max(0L, phase < peakMs ? cfg.burstPeakRatePerSec : cfg.burstBaseRatePerSec);
            }
            default:
                throw new IllegalStateException("Unhandled rate mode: " + cfg.rateMode);
        }
    }
}
