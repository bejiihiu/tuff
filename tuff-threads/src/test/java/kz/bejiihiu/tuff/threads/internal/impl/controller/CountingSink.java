package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;

/**
 * Sink that counts the only event the controller emits.
 */
final class CountingSink implements ThreadEventSink {

    private final AtomicInteger timeouts = new AtomicInteger();

    @Override
    public void threadCreated(final ThreadSnapshot thread) {
        Objects.requireNonNull(thread, "thread");
    }

    @Override
    public void threadTerminated(final ThreadSnapshot thread) {
        Objects.requireNonNull(thread, "thread");
    }

    @Override
    public void poolState(final PoolSnapshot pool) {
        Objects.requireNonNull(pool, "pool");
    }

    @Override
    public void saturation(final SaturationReport report) {
        Objects.requireNonNull(report, "report");
    }

    @Override
    public void leaseTimedOut(final LeaseRequest request) {
        Objects.requireNonNull(request, "request");
        this.timeouts.incrementAndGet();
    }

    @Override
    public void unregisteredThread(final long threadId, final String name) {
        Objects.requireNonNull(name, "name");
    }

    /**
     * Returns how many lease timeouts were reported.
     *
     * @return event count
     */
    int timeouts() {
        return this.timeouts.get();
    }
}
