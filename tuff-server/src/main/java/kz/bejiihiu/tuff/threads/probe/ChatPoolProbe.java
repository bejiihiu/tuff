package kz.bejiihiu.tuff.threads.probe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import net.minecraft.server.MinecraftServer;
import org.jspecify.annotations.Nullable;

/**
 * Observe-only probe of the chat decoration executor.
 *
 * <p>The executor is a cached {@code ThreadPoolExecutor} owned by {@code MinecraftServer}, and its core
 * and maximum size are a documented configuration key ({@code misc.chat-threads.*}). The probe reads the
 * executor's own counters - size, active count, queue depth - and finds the threads by their name,
 * because the executor does not expose them. It never touches the pool's sizing.
 *
 * <p>When the executor is absent (early startup or a server that never had one) the probe falls back to
 * the name scan and reports {@code -1} for what only the executor could answer.
 */
public final class ChatPoolProbe implements PoolProbe {

    /**
     * Name format of the chat threads, e.g. {@code Async Chat Thread - #0}.
     */
    static final String THREAD_PREFIX = "Async Chat Thread - #";

    private final PoolCountersSource counters;
    private final LiveThreadSource threads;

    /**
     * Creates the probe bound to the live server.
     */
    public ChatPoolProbe() {
        this(ChatPoolProbe::serverCounters, LiveThreadSource.jvm());
    }

    /**
     * Creates a probe with explicit sources, for tests.
     *
     * @param counters counter source
     * @param threads  thread source
     * @throws NullPointerException if a source is {@code null}
     */
    ChatPoolProbe(final PoolCountersSource counters, final LiveThreadSource threads) {
        this.counters = Objects.requireNonNull(counters, "counters");
        this.threads = Objects.requireNonNull(threads, "threads");
    }

    @Override
    public PoolId pool() {
        return PoolId.CHAT;
    }

    /**
     * Takes one reading of the executor.
     *
     * <p>Never throws: a missing server, a replaced executor or a dead thread table reads as
     * unavailable instead of an empty pool.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        final @Nullable List<Thread> matched = this.matchingThreads();
        final @Nullable PoolCounters measured = this.countersSafely();
        if (measured != null) {
            return ThreadPoolObservation.fromCounters(measured, matched == null ? List.of() : matched);
        }
        if (matched == null) {
            return ThreadPoolObservation.unavailable();
        }
        return ThreadPoolObservation.fromThreads(matched, 0, -1L, -1L);
    }

    private @Nullable List<Thread> matchingThreads() {
        try {
            final List<Thread> matched = new ArrayList<>();
            for (final Thread thread : this.threads.threads()) {
                if (thread != null && thread.getName() != null && thread.getName().startsWith(THREAD_PREFIX)) {
                    matched.add(thread);
                }
            }
            return matched;
        } catch (RuntimeException | LinkageError _) {
            return null;
        }
    }

    private @Nullable PoolCounters countersSafely() {
        try {
            return this.counters.counters();
        } catch (RuntimeException | LinkageError _) {
            return null;
        }
    }

    private static @Nullable PoolCounters serverCounters() {
        final MinecraftServer server = MinecraftServer.getServer();
        if (server == null) {
            return null;
        }
        if (server.chatExecutor instanceof ThreadPoolExecutor executor) {
            return new PoolCounters(
                executor.getPoolSize(),
                executor.getCorePoolSize(),
                executor.getActiveCount(),
                executor.getQueue().size()
            );
        }
        return null;
    }
}
