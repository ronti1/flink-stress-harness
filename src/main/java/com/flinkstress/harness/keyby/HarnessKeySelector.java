package com.flinkstress.harness.keyby;

import com.flinkstress.harness.config.PartitionMode;
import com.flinkstress.harness.model.Event;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key extraction strategy.
 *
 * <ul>
 *   <li>{@link PartitionMode#KEY_BY}: keys by the event's own key, so the
 *       hot-key skew baked into the data realises as load skew across subtasks.</li>
 *   <li>{@link PartitionMode#REBALANCE}: keys by {@code seqNo % numKeys}, an even
 *       spread that ignores data skew (the "shuffle, no skew" mode).</li>
 * </ul>
 */
public class HarnessKeySelector implements KeySelector<Event, String> {

    private static final long serialVersionUID = 1L;

    private final PartitionMode mode;
    private final long numKeys;

    public HarnessKeySelector(PartitionMode mode, long numKeys) {
        this.mode = mode;
        this.numKeys = numKeys;
    }

    @Override
    public String getKey(Event event) {
        if (mode == PartitionMode.REBALANCE) {
            long bucket = Math.floorMod(event.seqNo, numKeys);
            return "p" + bucket;
        }
        return event.key;
    }
}
