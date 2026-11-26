package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.config.ConfigDiff;

/**
 * Works out what a reload may do to the policies of the consumers that are already registered.
 *
 * <p>A reload must not be a reset. Three rules keep it narrow:
 * <ol>
 *     <li>only a consumer whose <em>effective configured</em> policy changed is a candidate at all;</li>
 *     <li>a consumer that a plugin registered through
 *     {@link AsyncComputeScheduler#registerConsumer(ComputeConsumer)} keeps the policy the plugin
 *     passed, unless the configuration has an explicit override for that id;</li>
 *     <li>a configured consumer that is not registered right now is reported as deferred instead of
 *     being counted as applied.</li>
 * </ol>
 *
 * <p>{@link #configuredPolicy(ComputeSettings, String)} repeats the rule the engine uses for the
 * consumers it registers itself: an explicit override wins, otherwise the default policy is used with
 * its overflow behaviour replaced by {@code queue.overflow-policy}. The engine does not expose that
 * mapping, so it is mirrored here; keep the two in sync when the inheritance rule changes.
 */
final class ConsumerReload {

    private static final String CONSUMER_PREFIX = "async-compute.consumers.";
    private static final String QUEUE_OVERFLOW_PATH = "async-compute.queue.overflow-policy";

    private ConsumerReload() {
    }

    /**
     * What one reload did to the registered consumers.
     *
     * @param applied  ids whose policy was replaced, sorted
     * @param deferred ids whose override changed but which are not registered, sorted
     */
    record Outcome(List<String> applied, List<String> deferred) {

        Outcome {
            applied = List.copyOf(applied);
            deferred = List.copyOf(deferred);
        }
    }

    /**
     * Applies the changed consumer policies to a running scheduler.
     *
     * @param scheduler running scheduler to update
     * @param previous  settings the engine was running with
     * @param updated   newly published settings
     * @return ids that were updated and ids that have to wait for their consumer
     */
    static Outcome apply(
        final AsyncComputeScheduler scheduler,
        final ComputeSettings previous,
        final ComputeSettings updated
    ) {
        final Set<String> candidates = new TreeSet<>(previous.consumers().overrides().keySet());
        candidates.addAll(updated.consumers().overrides().keySet());
        candidates.addAll(scheduler.consumerIds());

        final List<String> applied = new ArrayList<>();
        final List<String> deferred = new ArrayList<>();
        for (final String consumerId : candidates) {
            final ConsumerPolicy before = configuredPolicy(previous, consumerId);
            final ConsumerPolicy after = configuredPolicy(updated, consumerId);
            if (before.equals(after)) {
                // the config says nothing new about this consumer, so its live policy is none of our
                // business - this is what keeps a reload from touching plugin registrations
                continue;
            }
            final Optional<ComputeConsumer> live = scheduler.consumer(consumerId);
            if (live.isEmpty()) {
                deferred.add(consumerId);
                continue;
            }
            final boolean explicitOverride = updated.consumers().overrides().containsKey(consumerId)
                || previous.consumers().overrides().containsKey(consumerId);
            if (!explicitOverride && !isEngineManaged(scheduler, consumerId, live.get(), before)) {
                // the live policy was not set from the config, so it is the plugin's and it wins
                continue;
            }
            try {
                scheduler.updateConsumerPolicy(consumerId, after);
                applied.add(consumerId);
            } catch (IllegalArgumentException _) {
                // the consumer was unregistered between listing and updating
                deferred.add(consumerId);
            }
        }
        return new Outcome(applied, deferred);
    }

    /**
     * Returns the policy the configuration implies for a consumer id.
     *
     * <p>Mirrors the engine's inheritance: an explicit {@code consumers.<id>} section wins, every
     * other id gets {@code consumers.default} with the overflow behaviour replaced by
     * {@code queue.overflow-policy}.
     *
     * @param settings   snapshot to read
     * @param consumerId consumer to resolve
     * @return the effective configured policy
     */
    static ConsumerPolicy configuredPolicy(final ComputeSettings settings, final String consumerId) {
        final ConsumerPolicy explicit = settings.consumers().overrides().get(consumerId);
        if (explicit != null) {
            return explicit;
        }
        final ConsumerPolicy base = settings.consumers().defaultPolicy();
        final OverflowPolicy queueOverflow = settings.queue().overflowPolicy();
        if (base.overflowPolicy() == queueOverflow) {
            return base;
        }
        return new ConsumerPolicy(
            base.enabled(),
            base.concurrencyLimit(),
            base.maxQueuedTasks(),
            base.fairShareWeight(),
            queueOverflow,
            base.maxQueueWait()
        );
    }

    /**
     * Splits the changed keys into "applied now" and "has to wait".
     *
     * <p>A changed key is applied only when the consumer it belongs to is in
     * {@code appliedConsumerIds}. Everything that cannot change on a live pool - worker sizing,
     * metrics switches, queue capacity, scheduling - is deferred. An override for a consumer that is
     * not registered is left out here as well: it is already reported as a deferred consumer id and
     * would otherwise be described as "needs a restart", which it does not.
     *
     * @param diff                what changed between the two snapshots
     * @param appliedConsumerIds  ids the reload actually updated
     * @param deferredConsumerIds ids whose override is waiting for their consumer
     * @return changed keys that did not take effect, sorted
     */
    static List<String> deferredPaths(
        final ConfigDiff diff,
        final List<String> appliedConsumerIds,
        final List<String> deferredConsumerIds
    ) {
        final List<String> deferred = new ArrayList<>();
        for (final String path : diff.changedPaths()) {
            if (diff.restartRequiredPaths().contains(path) || !isLivePath(path)) {
                deferred.add(path);
                continue;
            }
            if (path.equals(QUEUE_OVERFLOW_PATH)) {
                if (appliedConsumerIds.isEmpty()) {
                    deferred.add(path);
                }
                continue;
            }
            final String consumerId = consumerIdOf(path);
            if (deferredConsumerIds.contains(consumerId)) {
                continue;
            }
            if (!appliedConsumerIds.contains(consumerId)) {
                deferred.add(path);
            }
        }
        return List.copyOf(deferred);
    }

    /**
     * Checks whether the live policy is still the one the config put there.
     *
     * <p>When the reload runs through {@link LimitedConsumerScheduler} the answer comes from its
     * record of explicit registrations. For a bare engine the comparison against the previous
     * configured policy is the best available signal: a policy that a plugin changed by hand no
     * longer equals it and is therefore left alone.
     *
     * @param scheduler  engine that holds the live consumers
     * @param consumerId consumer being considered
     * @param live       consumer as the engine stores it
     * @param before     policy the previous configuration implied
     * @return {@code true} when the engine registered the consumer from the configuration and the
     *     policy is unchanged
     */
    private static boolean isEngineManaged(
        final AsyncComputeScheduler scheduler,
        final String consumerId,
        final ComputeConsumer live,
        final ConsumerPolicy before
    ) {
        if (scheduler instanceof LimitedConsumerScheduler limited && limited.isExplicitlyRegistered(consumerId)) {
            return false;
        }
        return live.policy().equals(before);
    }

    private static boolean isLivePath(final String path) {
        return path.equals(QUEUE_OVERFLOW_PATH) || path.startsWith(CONSUMER_PREFIX);
    }

    private static String consumerIdOf(final String path) {
        final String remainder = path.substring(CONSUMER_PREFIX.length());
        final int dot = remainder.indexOf('.');
        return dot < 0 ? remainder : remainder.substring(0, dot);
    }
}
