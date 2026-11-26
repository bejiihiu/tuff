package kz.bejiihiu.tuff.threads.internal.runtime;

import java.util.Objects;
import java.util.function.Consumer;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.impl.events.JfrThreadSink;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.SystemMonotonicClock;
import kz.bejiihiu.tuff.threads.internal.impl.topology.TopologyProbe;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;
import org.jspecify.annotations.Nullable;

/**
 * Assembles a runtime from the parts a platform supplies.
 *
 * <p>The production path takes the system clock, the detected topology and the flight recorder sink.
 * The explicit constructor is for the server bootstrap, which may already own a topology reading, and
 * for tests, which need deterministic time and a recorder instead of JFR.
 *
 * <p>{@link #create(ThreadManagementSettings)} is side effect free on purpose: it builds objects and
 * reads the topology, but registers no pool and creates no thread. That is what makes a failed start
 * recoverable - there is nothing to clean up.
 */
public final class DefaultThreadManagerFactory implements ThreadManagerFactory {

    private final MonotonicClock clock;
    private final TopologyView topology;
    private final MetricsEventSink sink;
    private final @Nullable Consumer<String> stopSteps;

    /**
     * Creates the production factory.
     *
     * <p>Uses {@link System#nanoTime()} backed time, the running machine's topology and the flight
     * recorder sink. The sink degrades to a no-op on a runtime without {@code jdk.jfr}, so profiling
     * never decides whether the server starts.
     */
    public DefaultThreadManagerFactory() {
        this(new SystemMonotonicClock(), TopologyProbe.detect(), new JfrThreadSink(), null);
    }

    /**
     * Creates a factory with explicit seams.
     *
     * @param clock    monotonic time source for leases and sampling
     * @param topology processor topology to size against
     * @param sink     event sink, JFR in production
     * @throws NullPointerException if a reference is {@code null}
     */
    public DefaultThreadManagerFactory(
        final MonotonicClock clock,
        final TopologyView topology,
        final MetricsEventSink sink
    ) {
        this(clock, topology, sink, null);
    }

    /**
     * Creates a factory that reports shutdown steps.
     *
     * <p>Package private because the step observer is a test and diagnostics seam, not a public configuration
     * point.
     *
     * @param clock     monotonic time source for leases and sampling
     * @param topology  processor topology to size against
     * @param sink      event sink, JFR in production
     * @param stopSteps observer called once per shutdown step, or {@code null} to ignore them
     * @throws NullPointerException if a required reference is {@code null}
     */
    DefaultThreadManagerFactory(
        final MonotonicClock clock,
        final TopologyView topology,
        final MetricsEventSink sink,
        final @Nullable Consumer<String> stopSteps
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.topology = Objects.requireNonNull(topology, "topology");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.stopSteps = stopSteps;
    }

    @Override
    public ThreadManagerRuntime create(final ThreadManagementSettings settings) {
        return new ThreadManagerRuntimeImpl(
            Objects.requireNonNull(settings, "settings"),
            this.clock,
            this.topology,
            this.sink,
            this.stopSteps
        );
    }
}
