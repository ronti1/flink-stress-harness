package com.flinkstress.harness.window;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.TimeCharacteristic;
import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Builds the windowed aggregation stage from config, selecting between
 * tumbling-by-time, tumbling-by-count and session windows (event time or
 * processing time) and wiring the {@link WeightedAggregateFunction} plus
 * {@link KeyStampWindowFunction}.
 *
 * <p>{@code allowedLateness} only applies to event-time windows; processing-time
 * windows have no late-event concept.
 */
public final class WindowFactory {

    private WindowFactory() {
    }

    public static SingleOutputStreamOperator<AggResult> apply(
            KeyedStream<Event, String> keyed, HarnessConfig cfg) {

        WeightedAggregateFunction agg = new WeightedAggregateFunction(cfg);
        boolean eventTime = cfg.timeCharacteristic == TimeCharacteristic.EVENT_TIME;

        switch (cfg.windowType) {
            case TUMBLING_TIME:
                if (eventTime) {
                    return keyed
                            .window(TumblingEventTimeWindows.of(Time.milliseconds(cfg.windowSizeMs)))
                            .allowedLateness(Time.milliseconds(cfg.allowedLatenessMs))
                            .aggregate(agg, new KeyStampWindowFunction<TimeWindow>());
                }
                return keyed
                        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(cfg.windowSizeMs)))
                        .aggregate(agg, new KeyStampWindowFunction<TimeWindow>());

            case SESSION:
                if (eventTime) {
                    return keyed
                            .window(EventTimeSessionWindows.withGap(Time.milliseconds(cfg.sessionGapMs)))
                            .allowedLateness(Time.milliseconds(cfg.allowedLatenessMs))
                            .aggregate(agg, new KeyStampWindowFunction<TimeWindow>());
                }
                return keyed
                        .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(cfg.sessionGapMs)))
                        .aggregate(agg, new KeyStampWindowFunction<TimeWindow>());

            case TUMBLING_COUNT:
                return keyed
                        .countWindow(cfg.windowCountSize)
                        .aggregate(agg, new KeyStampWindowFunction<GlobalWindow>());

            default:
                throw new IllegalStateException("Unhandled window type: " + cfg.windowType);
        }
    }
}
