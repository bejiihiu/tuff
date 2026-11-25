package kz.bejiihiu.tuff.compute.config;

import kz.bejiihiu.tuff.compute.ComputeSettings;

/**
 * Apply hook between the configuration file and a running scheduler.
 *
 * <p>The configuration layer never touches the engine itself: it parses the file, validates it and
 * hands the previous and the new snapshot to this hook. An implementation applies what the scheduler
 * supports at runtime and reports the rest, which is exactly what {@link ConfigDiff#livePaths()} and
 * {@link ConfigDiff#restartRequiredPaths()} are for.
 *
 * <p>Typical implementation for the compute scheduler:
 * <ul>
 *     <li>worker sizing from {@code updated.workers()} through the engine's live resize entry
 *     point, which is what {@code Workers.resolveThreadCount} feeds;</li>
 *     <li>for every consumer id whose effective policy differs, call
 *     {@code AsyncComputeScheduler.updateConsumerPolicy(id, updated.consumers().policyFor(id))} -
 *     including consumers that lost their override, because they fall back to the default policy;</li>
 *     <li>metrics and adaptive switches from the respective sections;</li>
 *     <li>log {@link ConfigDiff#restartRequiredPaths()} as "takes effect after restart".</li>
 * </ul>
 *
 * <p>The hook is called while the configuration lock is held, so concurrent reloads cannot interleave.
 * It must not block for long and must not read the configuration back through a blocking path; the
 * snapshot accessor is lock free.
 */
@FunctionalInterface
public interface ComputeConfigApplyTarget {

    /**
     * Applies a configuration change to the engine.
     *
     * @param previous the snapshot the engine was running before the reload
     * @param updated  the new snapshot, already stored and readable through
     *                 {@link TuffComputeConfig#settings()}
     * @param diff     what changed between both snapshots
     */
    void apply(ComputeSettings previous, ComputeSettings updated, ConfigDiff diff);
}
