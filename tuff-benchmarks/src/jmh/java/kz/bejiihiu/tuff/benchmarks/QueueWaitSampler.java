package kz.bejiihiu.tuff.benchmarks;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Collects queue-wait samples from worker threads without locking or allocating per sample.
 *
 * <p>Every worker thread gets its own fixed ring, so the hot path is a nanoTime plus an array store.
 * Rings keep the last {@value #CAPACITY} samples; the benchmark prints percentiles from whatever is
 * still there, which is plenty for a distribution that is not a single spike.
 */
final class QueueWaitSampler {

    /** Samples kept per worker thread. */
    static final int CAPACITY = 1 << 14;

    private static final int MASK = CAPACITY - 1;

    private final ConcurrentLinkedQueue<Chunk> chunks = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Chunk> local = ThreadLocal.withInitial(() -> {
        final Chunk chunk = new Chunk();
        this.chunks.add(chunk);
        return chunk;
    });

    /**
     * Records one sample on the calling worker thread.
     *
     * @param nanos queue wait in nanoseconds
     */
    void record(final long nanos) {
        final Chunk chunk = this.local.get();
        chunk.values[(int) (chunk.total & MASK)] = nanos;
        chunk.total++;
    }

    /**
     * Merges every worker ring into one sorted array.
     *
     * @return all retained samples, ascending
     */
    long[] snapshot() {
        final long[] merged = new long[this.chunks.size() * CAPACITY];
        int position = 0;
        for (final Chunk chunk : this.chunks) {
            final int size = (int) Math.min(chunk.total, CAPACITY);
            position += size;
            // the samples are stored as a ring, so the newest size entries start at total
            final int start = (int) Math.max(0L, chunk.total - CAPACITY);
            for (int index = 0; index < size; index++) {
                merged[position - size + index] = chunk.values[(start + index) & MASK];
            }
        }
        final long[] result = Arrays.copyOf(merged, position);
        Arrays.sort(result);
        return result;
    }

    /**
     * Returns one percentile of the retained samples.
     *
     * @param sorted  ascending samples
     * @param quantile quantile in {@code [0, 1]}
     * @return sample at the quantile, {@code 0} for an empty array
     */
    static long percentile(final long[] sorted, final double quantile) {
        if (sorted.length == 0) {
            return 0L;
        }
        final int index = (int) Math.min(sorted.length - 1L, Math.round((sorted.length - 1L) * quantile));
        return sorted[index];
    }

    /**
     * Ring of one worker thread.
     */
    private static final class Chunk {

        private final long[] values = new long[CAPACITY];
        private long total;
    }
}
