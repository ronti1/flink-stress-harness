package com.flinkstress.harness.window;

import com.flinkstress.harness.model.AggResult;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Stamps the key and the aggregate-production wall-clock time onto the single
 * {@link AggResult} produced by {@link WeightedAggregateFunction} for a window.
 * Generic over the window type so it serves time and count (global) windows.
 */
public class KeyStampWindowFunction<W extends Window>
        implements WindowFunction<AggResult, AggResult, String, W> {

    private static final long serialVersionUID = 1L;

    @Override
    public void apply(String key, W window, Iterable<AggResult> input, Collector<AggResult> out) {
        AggResult r = input.iterator().next();
        r.key = key;
        r.aggTimeMs = System.currentTimeMillis();
        out.collect(r);
    }
}
