package com.flinkstress.harness.sink;

import com.flinkstress.harness.fault.FaultInjector;
import com.flinkstress.harness.model.AggResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/** Logs each aggregate to stdout/log. For debugging and no-Grafana runs. */
public class ConsoleSink extends RichSinkFunction<AggResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    private FaultInjector faultInjector = new FaultInjector(Collections.emptyList(), 0L);

    /** Wires an in-operator fault injector (SINK stage). */
    public ConsoleSink withFaultInjector(FaultInjector injector) {
        this.faultInjector = injector;
        return this;
    }

    @Override
    public void open(Configuration parameters) {
        faultInjector.open(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void invoke(AggResult value, Context context) {
        if (faultInjector.hasFaults()) {
            faultInjector.onRecord();
        }
        long e2e = value.maxEmitTimeMs > 0 ? System.currentTimeMillis() - value.maxEmitTimeMs : -1;
        LOG.info("AGG key={} count={} sumBytes={} e2eLatencyMs={}",
                value.key, value.count, value.sumBytes, e2e);
    }
}
