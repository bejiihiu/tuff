package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.QuotaController;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;

/**
 * Lock free quota controller backed by one atomic active counter per consumer.
 *
 * <p>A slot is reserved with a compare and set loop against the limit resolved from the consumer's
 * current policy and the current pool wide limit, so the configured bound is never exceeded even
 * when a hundred workers call {@link #tryAcquire(String)} at the same moment. Limits are resolved
 * lazily on every check: a policy update or a pool resize takes effect immediately, without having
 * to recompute stored per consumer values.
 *
 * <p>Lowering a limit never touches a running task. It only makes the next {@link #admits(String)}
 * or {@link #tryAcquire(String)} fail until enough slots were released, which is exactly what the
 * engine needs when adaptive concurrency shrinks the pool.
 */
public final class AtomicQuotaController implements QuotaController {

    private final ConcurrentHashMap<String, Quota> quotas = new ConcurrentHashMap<>();

    /**
     * Current pool wide limit. Volatile because the adaptive controller publishes new values from
     * its own thread while workers read it on every admission check.
     */
    private volatile int totalLimit;

    /**
     * Creates a controller.
     *
     * @param totalLimit pool wide maximum number of concurrently executing tasks, at least {@code 1}
     * @throws IllegalArgumentException if the limit is not positive
     */
    public AtomicQuotaController(final int totalLimit) {
        this.totalLimit = Preconditions.checkPositive(totalLimit, "totalLimit");
    }

    @Override
    public boolean admits(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        return quota != null && quota.active.get() < this.limitOf(quota);
    }

    @Override
    public boolean tryAcquire(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        if (quota == null) {
            return false;
        }
        while (true) {
            final int active = quota.active.get();
            if (active >= this.limitOf(quota)) {
                return false;
            }
            if (quota.active.compareAndSet(active, active + 1)) {
                return true;
            }
        }
    }

    @Override
    public void release(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        if (quota == null) {
            return;
        }
        while (true) {
            final int active = quota.active.get();
            if (active == 0) {
                // over release is a caller bug; clamping is still better than a negative counter that
                // would silently grant extra slots forever.
                return;
            }
            if (quota.active.compareAndSet(active, active - 1)) {
                return;
            }
        }
    }

    @Override
    public int activeTasks(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        return quota == null ? 0 : quota.active.get();
    }

    @Override
    public int activeTasks() {
        int total = 0;
        for (final Quota quota : this.quotas.values()) {
            total += quota.active.get();
        }
        return total;
    }

    @Override
    public int limit(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        return quota == null ? 0 : this.limitOf(quota);
    }

    @Override
    public int totalLimit() {
        return this.totalLimit;
    }

    @Override
    public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Preconditions.requireNonBlank(consumerId, "consumerId");
        final Quota existing = this.quotas.get(consumerId);
        if (existing != null) {
            existing.policy = policy;
            return;
        }
        final Quota raced = this.quotas.putIfAbsent(consumerId, new Quota(policy));
        if (raced != null) {
            raced.policy = policy;
        }
    }

    @Override
    public void removeConsumer(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota removed = this.quotas.remove(consumerId);
        if (removed == null) {
            return;
        }
        final int active = removed.active.get();
        if (active != 0) {
            // put the accounting back so the engine can still release the outstanding slots instead
            // of leaking them.
            this.quotas.putIfAbsent(consumerId, removed);
            throw new IllegalStateException(
                "consumer " + consumerId + " still has " + active + " active task(s)"
            );
        }
    }

    @Override
    public void updateTotalLimit(final int totalLimit) {
        this.totalLimit = Preconditions.checkPositive(totalLimit, "totalLimit");
    }

    @Override
    public void reset() {
        for (final Quota quota : this.quotas.values()) {
            quota.active.set(0);
        }
    }

    /**
     * Resolves the effective limit of one consumer against the current pool limit.
     *
     * @param quota quota to resolve
     * @return the number of tasks that may run concurrently, at least {@code 1}
     */
    private int limitOf(final Quota quota) {
        return quota.policy.resolveConcurrencyLimit(this.totalLimit);
    }

    /**
     * Mutable quota state of one consumer.
     */
    private static final class Quota {

        private final AtomicInteger active = new AtomicInteger();
        private volatile ConsumerPolicy policy;

        /**
         * Creates a quota.
         *
         * @param policy initial policy
         */
        Quota(final ConsumerPolicy policy) {
            this.policy = policy;
        }
    }
}
