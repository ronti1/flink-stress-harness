package com.flinkstress.harness.fault;

import java.util.concurrent.atomic.AtomicInteger;

/** Test halt strategy that records calls instead of killing the JVM. */
public class RecordingHaltStrategy implements HaltStrategy {

    private static final long serialVersionUID = 1L;

    public final AtomicInteger calls = new AtomicInteger();
    public volatile int lastExitCode = Integer.MIN_VALUE;

    @Override
    public void halt(int exitCode) {
        lastExitCode = exitCode;
        calls.incrementAndGet();
    }
}
