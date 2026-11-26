package kz.bejiihiu.tuff.threads.internal.testing;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;

/**
 * Sink that remembers what happened instead of recording it.
 */
public final class RecordingEventSink implements ThreadEventSink {

    private final AtomicInteger created = new AtomicInteger();
    private final AtomicInteger terminated = new AtomicInteger();
    private final AtomicInteger unregistered = new AtomicInteger();
    private final ConcurrentLinkedQueue<String> unregisteredNames = new ConcurrentLinkedQueue<>();

    @Override
    public void threadCreated(final ThreadSnapshot thread) {
        this.created.incrementAndGet();
    }

    @Override
    public void threadTerminated(final ThreadSnapshot thread) {
        this.terminated.incrementAndGet();
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
    }

    @Override
    public void unregisteredThread(final long threadId, final String name) {
        this.unregistered.incrementAndGet();
        this.unregisteredNames.add(name);
    }

    /**
     * Returns how many creation events were seen.
     *
     * @return event count
     */
    public int created() {
        return this.created.get();
    }

    /**
     * Returns how many termination events were seen.
     *
     * @return event count
     */
    public int terminated() {
        return this.terminated.get();
    }

    /**
     * Returns how many unregistered thread events were seen.
     *
     * @return event count
     */
    public int unregistered() {
        return this.unregistered.get();
    }

    /**
     * Returns the names reported as unregistered.
     *
     * @return a snapshot of reported names
     */
    public List<String> unregisteredNames() {
        return List.copyOf(this.unregisteredNames);
    }
}
