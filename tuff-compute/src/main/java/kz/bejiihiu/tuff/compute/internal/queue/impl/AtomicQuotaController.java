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
 * current policy and the current execution capacity, so the configured bound is never exceeded even
 * when a hundred workers call {@link #tryAcquire(String)} at the same moment. Limits are resolved
 * lazily on every check: a policy update, a pool resize or a virtual lane limit change takes effect
 * immediately, without having to recompute stored per consumer values.
 *
 * <p>Lowering a limit never touches a running task. It only makes the next {@link #admits(String)}
 * or {@link #tryAcquire(String)} fail until enough slots were released, which is exactly what the
 * engine needs when adaptive concurrency shrinks the pool.
 */
public final class AtomicQuotaController implements QuotaController {

    private final ConcurrentHashMap<String, Quota> quotas = new ConcurrentHashMap<>();

    /**
     * Current execution capacity relative limits are resolved against. Volatile because the adaptive
     * controller publishes new values from its own thread while workers read it on every admission
     * check.
     */
    private volatile int totalLimit;

    /**
     * Creates a controller.
     *
     * @param totalLimit execution capacity relative limits are resolved against, at least {@code 1}
     * @throws IllegalArgumentException if the limit is not positive
     */
    public AtomicQuotaController(final int totalLimit) {
        this.totalLimit = Preconditions.checkPositive(totalLimit, "totalLimit");
    }

    @Override
    public boolean admits(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        return quota != null && !quota.retired && quota.active.get() < this.limitOf(quota);
    }

    @Override
    public boolean tryAcquire(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        if (quota == null || quota.retired) {
            return false;
        }
        while (true) {
            final int active = quota.active.get();
            if (active >= this.limitOf(quota)) {
                return false;
            }
            if (quota.active.compareAndSet(active, active + 1)) {
                if (quota.retired) {
                    // removeConsumer marked it while this acquire was in flight; undo the slot so
                    // the retired object does not carry a phantom reservation
                    quota.active.decrementAndGet();
                    return false;
                }
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
            // a re-registration revives the same quota object, so outstanding slots of the previous
            // registration keep counting against the new limit instead of vanishing
            existing.retired = false;
            return;
        }
        final Quota raced = this.quotas.putIfAbsent(consumerId, new Quota(policy));
        if (raced != null) {
            raced.policy = policy;
            raced.retired = false;
        }
    }

    @Override
    public void removeConsumer(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final Quota quota = this.quotas.get(consumerId);
        if (quota == null) {
            return;
        }
        // mark first, then check: an acquire that already has the object either sees the flag and
        // rolls back, or bumps the counter before the check below keeps the object alive
        quota.retired = true;
        final int active = quota.active.get();
        if (active != 0) {
            // keep the very same object in the map so the outstanding releases still land here. do
            // not putIfAbsent a fresh quota: that would zero the live count.
            throw new IllegalStateException(
                "consumer " + consumerId + " still has " + active + " active task(s)"
            );
        }
        this.quotas.remove(consumerId, quota);
    }

    @Override
    public void updateTotalLimit(final int executionCapacity) {
        this.totalLimit = Preconditions.checkPositive(executionCapacity, "executionCapacity");
    }

    @Override
    public void reset() {
        for (final Quota quota : this.quotas.values()) {
            quota.active.set(0);
        }
    }

    /**
     * Resolves the effective limit of one consumer against the current execution capacity.
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
         * Set while the engine is removing the consumer. Kept on the same object so an in-flight
         * acquire/release cannot be lost into a replacement quota.
         */
        private volatile boolean retired;

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
