package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.PoolId;

/**
 * One granted permit.
 *
 * <p>Everything except the released flag is fixed at grant time. The flag is what makes release
 * idempotent: the first caller wins the compare and set and returns the permit, every later caller is a
 * no-op. That also means a double release can never hand the same permit out twice, which is the leak
 * this handle exists to make impossible.
 *
 * <p>Package private: the only way to obtain one is {@link ResourceControllerImpl#acquire}.
 */
final class LeaseImpl implements Lease {

    private final ResourceControllerImpl owner;
    private final long uid;
    private final PoolId pool;
    private final long grantedNanos;
    private final long deadlineNanos;
    private final boolean global;
    private final AtomicBoolean released = new AtomicBoolean();

    /**
     * Creates a granted lease.
     *
     * @param owner         controller that granted it
     * @param uid           monotonic lease id
     * @param pool          pool the permit came from
     * @param grantedNanos  grant moment on the manager clock
     * @param deadlineNanos deadline copied from the request
     * @param global        whether the permit also consumed shared background admission
     */
    LeaseImpl(
        final ResourceControllerImpl owner,
        final long uid,
        final PoolId pool,
        final long grantedNanos,
        final long deadlineNanos,
        final boolean global
    ) {
        this.owner = owner;
        this.uid = uid;
        this.pool = pool;
        this.grantedNanos = grantedNanos;
        this.deadlineNanos = deadlineNanos;
        this.global = global;
    }

    /**
     * Returns the controller that granted this lease.
     *
     * @return the owner
     */
    ResourceControllerImpl owner() {
        return this.owner;
    }

    @Override
    public long uid() {
        return this.uid;
    }

    @Override
    public PoolId pool() {
        return this.pool;
    }

    @Override
    public long grantedNanos() {
        return this.grantedNanos;
    }

    @Override
    public long deadlineNanos() {
        return this.deadlineNanos;
    }

    @Override
    public boolean isReleased() {
        return this.released.get();
    }

    @Override
    public void release() {
        if (this.released.compareAndSet(false, true)) {
            this.owner.releasePermit(this.pool, this.global);
        }
    }
}
