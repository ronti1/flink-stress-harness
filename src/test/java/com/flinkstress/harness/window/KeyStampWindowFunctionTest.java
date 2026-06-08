package com.flinkstress.harness.window;

import com.flinkstress.harness.model.AggResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KeyStampWindowFunctionTest {

    @Test
    void stampsKeyAndEmitsSingleResult() {
        KeyStampWindowFunction<GlobalWindow> wf = new KeyStampWindowFunction<>();
        AggResult partial = new AggResult(null, 7, 70, 12345L, 0L);

        List<AggResult> out = new ArrayList<>();
        Collector<AggResult> collector = new Collector<AggResult>() {
            @Override
            public void collect(AggResult record) {
                out.add(record);
            }

            @Override
            public void close() {
            }
        };

        wf.apply("myKey", GlobalWindow.get(), Collections.singletonList(partial), collector);

        assertThat(out).hasSize(1);
        assertThat(out.get(0).key).isEqualTo("myKey");
        assertThat(out.get(0).count).isEqualTo(7);
        assertThat(out.get(0).aggTimeMs).isGreaterThan(0L);
    }
}
