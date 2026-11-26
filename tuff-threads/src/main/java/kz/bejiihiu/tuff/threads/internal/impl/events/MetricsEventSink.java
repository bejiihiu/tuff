package kz.bejiihiu.tuff.threads.internal.impl.events;

import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;

/**
 * The metrics layer's event sink: the frozen contract plus the budget transition.
 *
 * <p>{@link ThreadEventSink} is frozen and has no budget hook, but a budget change is exactly the kind
 * of event an operator needs to line up with a latency jump. Adding a method to a frozen interface is
 * not allowed, so the extra signal lives in this sub interface and the sampler depends on it. Any sink
 * that only implements the frozen contract is still usable through a small adapter that ignores the
 * budget signal.
 *
 * <p>Implementations must stay non throwing and cheap, like the contract says: the sampler calls a sink
 * on every pass and must never be slowed down or broken by an event.
 */
public interface MetricsEventSink extends ThreadEventSink {

    /**
     * Called when a registered pool's declared budget differs from the previous sampling pass.
     *
     * @param pool     pool whose budget moved
     * @param previous budget seen on the previous pass
     * @param current  budget seen now
     */
    void budgetChanged(PoolId pool, ResourceBudget previous, ResourceBudget current);
}
