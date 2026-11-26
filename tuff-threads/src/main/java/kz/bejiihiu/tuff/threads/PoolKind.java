package kz.bejiihiu.tuff.threads;

/**
 * Category of an execution pool.
 *
 * <p>The kind says what the pool is for, not who owns it and not how it is sized. The registry uses it
 * for grouping and for policy defaults; the resource controller uses it to decide which pools are
 * critical (region execution and network) and which may yield cores under pressure. A pool keeps its
 * kind for its whole life, and two pools of the same kind are not merged or aliased in any way.
 */
public enum PoolKind {

    /**
     * The fixed pool that runs region ticks and in between tick tasks.
     *
     * <p>Critical by definition: its threads carry region context and ownership checks, so it is never
     * shared with compute work and never shrunk while the server runs.
     */
    REGION_EXECUTION,

    /**
     * The pool of platform workers that execute background compute tasks.
     *
     * <p>Elastic: it may be resized against the remaining processor budget, and it is the pool that
     * yields cores when region work needs more.
     */
    COMPUTE,

    /**
     * Chunk generation, lighting, loading and saving workers.
     */
    CHUNK,

    /**
     * Blocking file and region file IO workers.
     */
    IO,

    /**
     * Network event loops and other socket oriented pools.
     *
     * <p>Specialised: an event loop owns its channels, so it is registered and observed but never
     * pooled together with CPU work.
     */
    NETWORK,

    /**
     * Short lived service pools such as command building, chat decoration, watchdog helpers and
     * per feature maintenance timers.
     */
    SERVICE,

    /**
     * Virtual thread lanes.
     *
     * <p>Virtual threads are not owned by UTMI in the JVM sense: the JDK scheduler owns the carriers.
     * The kind exists so the lane appears in snapshots, lease accounting and budget reports as a
     * distinct consumer of the same CPU.
     */
    VIRTUAL
}
