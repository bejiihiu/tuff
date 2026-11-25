package kz.bejiihiu.tuff.compute.config;

/**
 * How much CPU the compute scheduler may claim on a Folia server.
 *
 * <p>The JVM reports every core the machine has, but a regionised server already spends most of them
 * before the compute pool starts: one tick thread per region, the global tick thread, Moonrise chunk
 * workers, chunk IO threads and the Netty event loops. Feeding {@code Runtime.availableProcessors()}
 * into automatic worker sizing would therefore oversubscribe the machine and make tick times worse,
 * so the pool is sized from what is left after those threads are subtracted.
 *
 * <p>The formula mirrors the sizing guidance of the regioniser's own README: keep the total number
 * of threads below about 80% of the available cores, then hand the remainder to the compute pool.
 * The result is a <em>budget</em>, not a promise: it is what automatic sizing treats as
 * "available processors", so a pool configured with {@code threads: -1} picks {@code budget - 1}
 * workers, and {@code min-threads} / {@code max-threads} still clamp it.
 */
public final class ComputeThreadBudget {

    /**
     * Share of the cores the whole server should stay below. The regioniser documentation recommends
     * keeping total thread count under 80% of the available processors.
     */
    public static final double USABLE_CORE_FRACTION = 0.8d;

    /**
     * Threads reserved for the Netty event loops and the remaining small server pools that are not
     * counted by the caller.
     */
    public static final int NETWORK_RESERVE = 4;

    private ComputeThreadBudget() {
    }

    /**
     * Resolves the CPU budget for the compute pool.
     *
     * <p>{@code regionThreads} comes from the region scheduler
     * ({@code TickRegions.getScheduler().getTotalThreadCount()} once global configuration is
     * initialised), {@code chunkWorkerThreads} from the chunk system's worker thread count and
     * {@code ioThreads} from its IO pool. Pass {@code 0} for a source that does not exist yet; the
     * ordering of startup means the numbers are only reliable after the region scheduler is up, so
     * resolving the budget is meant to happen at that point, not in the {@code CraftServer}
     * constructor.
     *
     * @param availableProcessors processors reported by the JVM, at least {@code 1}
     * @param regionThreads       tick threads already allocated by the region scheduler, including
     *                            the global tick thread, not negative
     * @param chunkWorkerThreads  Moonrise chunk system worker threads, not negative
     * @param ioThreads           chunk system IO threads, not negative
     * @return processors the compute pool may use, always at least {@code 1}
     * @throws IllegalArgumentException if {@code availableProcessors} is not positive or any thread
     *                                  count is negative
     */
    public static int resolve(
        final int availableProcessors,
        final int regionThreads,
        final int chunkWorkerThreads,
        final int ioThreads
    ) {
        if (availableProcessors < 1) {
            throw new IllegalArgumentException("availableProcessors must be at least 1, got " + availableProcessors);
        }
        if (regionThreads < 0) {
            throw new IllegalArgumentException("regionThreads must not be negative, got " + regionThreads);
        }
        if (chunkWorkerThreads < 0) {
            throw new IllegalArgumentException("chunkWorkerThreads must not be negative, got " + chunkWorkerThreads);
        }
        if (ioThreads < 0) {
            throw new IllegalArgumentException("ioThreads must not be negative, got " + ioThreads);
        }

        final int usable = (int) Math.floor(availableProcessors * USABLE_CORE_FRACTION);
        final int remaining = usable - regionThreads - chunkWorkerThreads - ioThreads - NETWORK_RESERVE;
        return Math.max(1, remaining);
    }

    /**
     * Resolves the CPU budget on a server that does not report its own thread counts, for example a
     * test harness or a single threaded launcher. It applies the same 80% rule and only subtracts
     * the network reserve.
     *
     * @param availableProcessors processors reported by the JVM, at least {@code 1}
     * @return processors the compute pool may use, always at least {@code 1}
     * @throws IllegalArgumentException if {@code availableProcessors} is not positive
     */
    public static int resolve(final int availableProcessors) {
        return resolve(availableProcessors, 0, 0, 0);
    }
}
