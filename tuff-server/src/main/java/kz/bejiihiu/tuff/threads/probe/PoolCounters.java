package kz.bejiihiu.tuff.threads.probe;

/**
 * The cheap counters a pool's own API can answer.
 *
 * <p>Used where the owner exposes size and activity but not the thread list: a
 * {@code ThreadPoolExecutor} and a {@code ForkJoinPool} both know their size without a lock, while
 * their threads can only be found by name.
 *
 * @param alive  live threads reported by the pool
 * @param core   threads the pool keeps regardless of load, or {@code -1} when it has no such concept
 * @param active threads currently executing a task
 * @param queued tasks waiting in the pool queue, or {@code -1} when it does not expose a depth
 */
record PoolCounters(int alive, int core, int active, long queued) {
}
