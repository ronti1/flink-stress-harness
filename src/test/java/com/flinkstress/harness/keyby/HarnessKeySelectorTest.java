package com.flinkstress.harness.keyby;

import com.flinkstress.harness.config.PartitionMode;
import com.flinkstress.harness.model.Event;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HarnessKeySelectorTest {

    private static Event event(long seqNo, String key) {
        Event e = new Event();
        e.seqNo = seqNo;
        e.key = key;
        return e;
    }

    @Test
    void keyByUsesEventKey() {
        HarnessKeySelector sel = new HarnessKeySelector(PartitionMode.KEY_BY, 1000);
        assertThat(sel.getKey(event(5, "k42"))).isEqualTo("k42");
    }

    @Test
    void rebalanceSpreadsBySeqNoModuloNumKeys() {
        HarnessKeySelector sel = new HarnessKeySelector(PartitionMode.REBALANCE, 8);
        assertThat(sel.getKey(event(0, "kHot"))).isEqualTo("p0");
        assertThat(sel.getKey(event(9, "kHot"))).isEqualTo("p1");
        assertThat(sel.getKey(event(15, "kHot"))).isEqualTo("p7");
    }

    @Test
    void rebalanceHandlesNegativeSeqNoWithFloorMod() {
        HarnessKeySelector sel = new HarnessKeySelector(PartitionMode.REBALANCE, 8);
        assertThat(sel.getKey(event(-1, "x"))).isEqualTo("p7");
    }
}
