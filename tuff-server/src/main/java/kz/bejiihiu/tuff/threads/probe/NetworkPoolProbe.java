package kz.bejiihiu.tuff.threads.probe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;

/**
 * Observe-only probe of the netty event loops.
 *
 * <p>Netty keeps its groups private and an event loop owns its channels, so there is nothing safe to
 * ask it through an API: the only stable handle is the thread name. Both the player transport and the
 * management server name their loops predictably, so the probe filters the live thread table by name
 * and classifies the matches by {@link Thread#getState()}.
 *
 * <p>Event loops are created lazily, on the first connection; {@code alive 0} therefore means "no loop
 * has been created yet", not "the probe failed". Queue depth is {@code -1}: netty does not expose it
 * and a zero would look like an empty queue.
 */
public final class NetworkPoolProbe implements PoolProbe {

    /**
     * Prefix shared by the player transport event loops, e.g. {@code Netty NIO IO #0}.
     */
    static final String NETTY_PREFIX = "Netty ";

    /**
     * Marker inside every player transport loop name.
     */
    static final String IO_MARKER = " IO #";

    /**
     * Prefix of the JSON-RPC management server loops, when that server is enabled.
     */
    static final String MANAGEMENT_PREFIX = "Management server IO #";

    private final LiveThreadSource source;

    /**
     * Creates the probe bound to the live thread table.
     */
    public NetworkPoolProbe() {
        this(LiveThreadSource.jvm());
    }

    /**
     * Creates a probe with an explicit thread source, for tests.
     *
     * @param source thread source
     * @throws NullPointerException if {@code source} is {@code null}
     */
    NetworkPoolProbe(final LiveThreadSource source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public PoolId pool() {
        return PoolId.NETWORK;
    }

    /**
     * Takes one reading of the network loops.
     *
     * <p>Never throws: a thread table that cannot be enumerated reads as an unavailable source, which
     * the report prints as {@code unknown} rather than as an idle pool.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        try {
            final List<Thread> matched = new ArrayList<>();
            for (final Thread thread : this.source.threads()) {
                if (thread != null && isEventLoop(thread.getName())) {
                    matched.add(thread);
                }
            }
            return ThreadPoolObservation.fromThreads(matched, 0, -1L, -1L);
        } catch (RuntimeException | LinkageError _) {
            return ThreadPoolObservation.unavailable();
        }
    }

    /**
     * Checks whether a name belongs to a netty event loop.
     *
     * @param name thread name
     * @return {@code true} for a player transport or management server loop
     */
    static boolean isEventLoop(final String name) {
        if (name == null) {
            return false;
        }
        if (name.startsWith(MANAGEMENT_PREFIX)) {
            return true;
        }
        return name.startsWith(NETTY_PREFIX) && name.contains(IO_MARKER);
    }
}
