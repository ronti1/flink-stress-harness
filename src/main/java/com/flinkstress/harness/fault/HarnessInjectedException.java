package com.flinkstress.harness.fault;

/** Thrown by the application-exception fault to trigger a Flink region failover. */
public class HarnessInjectedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public HarnessInjectedException(String message) {
        super(message);
    }
}
