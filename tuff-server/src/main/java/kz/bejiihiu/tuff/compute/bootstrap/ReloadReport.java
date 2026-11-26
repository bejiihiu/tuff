package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.config.ConfigLoadResult;

/**
 * Outcome of a {@code /tuff compute reload}, split into what changed now and what has to wait.
 *
 * <p>The configuration layer reports every changed key in one list. The scheduler, however, can only
 * adopt a new consumer policy on a live pool: worker sizing, queue capacity and the metrics switches
 * are baked into the engine that was created at startup, because {@code AsyncComputeScheduler}
 * exposes its settings as an immutable snapshot. Even a consumer policy is applied only when that
 * consumer is registered at the moment; an override for an id nobody uses is kept for when it
 * registers, not reported as applied. The report keeps the groups apart instead of pretending that
 * everything was applied.
 *
 * @param result               what the configuration layer read, diffed and reported
 * @param appliedConsumerIds   consumer ids whose policy was replaced on the running engine
 * @param deferredConsumerIds  consumer ids whose override was saved but which are not registered
 * @param deferredPaths        changed keys that did not take effect
 */
public record ReloadReport(
    ConfigLoadResult result,
    List<String> appliedConsumerIds,
    List<String> deferredConsumerIds,
    List<String> deferredPaths
) {

    /**
     * Validates and freezes the report.
     *
     * @param result              what the configuration layer read, diffed and reported
     * @param appliedConsumerIds  consumer ids whose policy was replaced on the running engine
     * @param deferredConsumerIds consumer ids whose override was saved but which are not registered
     * @param deferredPaths       changed keys that did not take effect
     * @throws NullPointerException if a component is {@code null}
     */
    public ReloadReport {
        Objects.requireNonNull(result, "result");
        appliedConsumerIds = List.copyOf(appliedConsumerIds);
        deferredConsumerIds = List.copyOf(deferredConsumerIds);
        deferredPaths = List.copyOf(deferredPaths);
    }
}
