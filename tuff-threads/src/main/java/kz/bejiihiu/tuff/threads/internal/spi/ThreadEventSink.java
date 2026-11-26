package kz.bejiihiu.tuff.threads.internal.spi;

import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;

/**
 * Sink for thread lifecycle and pressure events.
 *
 * <p>The manager emits events whether or not anyone records them; the sink decides what to do with them.
 * The production implementation writes JFR events, which costs nothing while no recording is running, so
 * there is no reason to gate emission on a recording being active. Tests use an in memory sink to assert
 * that the manager reports what it should.
 *
 * <p>Emission happens from the sampler, the factory and the lease path. Implementations must be cheap,
 * non blocking and non throwing: a broken sink must never break thread creation or a lease. Exceptions
 * thrown by a sink are caught and rate limited by the manager, not propagated.
 */
public interface ThreadEventSink {

    /**
     * Called when a thread was created or adopted by the manager.
     *
     * @param thread snapshot of the new thread
     */
    void threadCreated(ThreadSnapshot thread);

    /**
     * Called when a registered thread terminated.
     *
     * @param thread snapshot captured at termination
     */
    void threadTerminated(ThreadSnapshot thread);

    /**
     * Called once per sampling pass for every pool.
     *
     * @param pool pool state at the end of the pass
     */
    void poolState(PoolSnapshot pool);

    /**
     * Called when a pool's saturation level changes or stays saturated for a full window.
     *
     * @param report the new report
     */
    void saturation(SaturationReport report);

    /**
     * Called when a lease request timed out.
     *
     * @param request the request that was not granted
     */
    void leaseTimedOut(LeaseRequest request);

    /**
     * Called for a thread that exists in the JVM but is not accounted to any pool.
     *
     * @param threadId JVM thread id
     * @param name     thread name at scan time
     */
    void unregisteredThread(long threadId, String name);
}
