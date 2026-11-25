package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QueueSnapshot;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import org.jspecify.annotations.Nullable;

/**
 * The bounded, multi consumer queue used by the compute engine.
 *
 * <p>The queue is a set of per consumer streams, and every stream is four sorted buckets, one per
 * {@link ComputePriority}. A bucket is ordered by submission sequence, so its first element is both
 * the oldest and the most aged entry of that priority. Because a task's aging only depends on how
 * long it waited, effective priority is monotone along a bucket: the head of a bucket always has
 * the highest effective priority of that bucket. The next entry of a consumer is therefore the best
 * of its four bucket heads, and the next entry of the queue is the best of the consumer heads.
 *
 * <p>That layout is what makes all four ordering inputs cheap without a background timer:
 * <ul>
 *     <li>priority is the bucket;</li>
 *     <li>aging is recomputed from the head's submission timestamp on every poll, so a waiting task
 *     climbs levels by itself and never needs to be moved or re-prioritised;</li>
 *     <li>weighted fairness is a fixed point virtual service counter per consumer, charged when a
 *     consumer is dispatched, so a weight of {@code 2.0} gets roughly twice the turns of
 *     {@code 1.0}. A consumer that was idle is aligned to the least served backlogged consumer when
 *     it returns, so an hour of unused turns cannot turn into an hour of monopoly;</li>
 *     <li>FIFO between equal entries is the sort key inside the bucket.</li>
 * </ul>
 *
 * <p>Effective priority is monotone along a bucket because both the sequence and the submission
 * timestamp grow with the submission order; a nanosecond scale inversion around an aging boundary can
 * only move one entry by one level, and the sequence tie break keeps the result deterministic.
 *
 * <p>Nothing leaves the queue unnoticed. Every path that removes an entry hands it back to the
 * scheduler: {@link #poll(ConsumerAdmission)} and {@link #steal(int, ConsumerAdmission)} return it,
 * {@link #remove(QueueEntry)} reports it, {@link #drainTo(Collection, int)},
 * {@link #drainExpired(long, Collection)}, {@link #clear()} and {@link #unregisterConsumer(String)}
 * return collections, and a {@link OverflowPolicy#DROP_OLDEST} eviction reports the entry to the
 * installed {@link EvictionListener} on the offering thread before {@code offer} returns. The queue
 * never completes a handle, because only the scheduler owns the futures.
 *
 * <p>The hot path takes no locks. Producers and consumers only use compare and set on counters,
 * {@link java.util.concurrent.ConcurrentSkipListMap} operations and a volatile snapshot of the
 * consumer array; the only mutex is a short one for consumer registration and removal, which is
 * topology work and never runs on a task path. Removal and eviction are {@code O(log n)}, selection
 * is {@code O(c log m)} for {@code c} consumers and {@code m} entries of one consumer, and
 * {@code c} is the number of registered subsystems, normally a handful.
 *
 * <p>The queue deliberately does not reuse LeafPile's {@code PrioritisedTaskQueue}: that structure is
 * unbounded, offers no eviction, and its ordering key cannot carry an aging computation or a virtual
 * service time, so every missing feature would have to be rebuilt next to it. It is also not
 * reachable from this module, which must stay free of server dependencies.
 */
public final class BoundedComputeQueue implements LaneAwareQueue {

    /**
     * Number of priority levels; must match {@link ComputePriority}.
     */
    private static final int LEVELS = 4;

    /**
     * Mask that includes every priority level.
     */
    private static final int ALL_LEVELS = (1 << LEVELS) - 1;

    /**
     * Fixed point scale of one dispatch turn in virtual service time. One point is one millionth of
     * a turn, which keeps a two to one weight ratio exact while staying far away from overflow.
     */
    private static final long FAIRNESS_SCALE = 1_000_000L;

    /**
     * Upper bound of one dispatch charge. A pathological weight below {@code 1e-6} would otherwise
     * overflow the fixed point counter in a few dispatches.
     */
    private static final long MAX_DISPATCH_CHARGE = 1L << 40;

    /**
     * Virtual service value that triggers a rebase. At one million points per dispatch this is far
     * beyond any realistic run before the subtraction happens.
     */
    private static final long REBASE_THRESHOLD = 1L << 52;

    /**
     * How often {@link #offer(QueueEntry)} may lose a race with consumer removal before it refuses.
     * Each retry means the consumer was unregistered and re-registered while the entry was in flight.
     */
    private static final int MAX_RETIREMENT_RACES = 8;

    /**
     * Lookup key below every real key, used to read the first element of a bucket without allocating
     * a map entry or throwing on an empty bucket.
     */
    private static final Node LOOKUP_MIN = new Node(Long.MIN_VALUE, Long.MIN_VALUE);

    /**
     * Admission check for introspection methods, which have no admission parameter.
     */
    private static final ConsumerAdmission ALWAYS_ADMIT = consumerId -> true;

    /**
     * Bucket order: submission sequence, then task id so two submissions can never compare equal.
     */
    private static final Comparator<Node> SEQUENCE_ORDER = (left, right) -> {
        final int bySequence = Long.compare(left.sequence, right.sequence);
        return bySequence != 0 ? bySequence : Long.compare(left.taskId, right.taskId);
    };

    private final int capacity;
    private final ConsumerPolicy defaultPolicy;
    private final ComputeSettings.Scheduling.Strategy strategy;
    private final boolean priorityAging;
    private final long agingIntervalNanos;
    private final int maxAgingBoost;
    private final boolean workStealing;
    private final ComputeClock clock;

    private final ConcurrentHashMap<String, ConsumerState> consumers = new ConcurrentHashMap<>();
    private final Object topologyLock = new Object();

    /**
     * Copy on write view of the registered consumers, so a poll never allocates an iterator.
     */
    private volatile ConsumerState[] streams = new ConsumerState[0];

    private final AtomicInteger queued = new AtomicInteger();

    /**
     * Queued entries per priority level. Maintained on offer and unlink so lane dispatch and the
     * virtual lane gauges never have to walk the consumers.
     */
    private final AtomicInteger[] levelCounts = new AtomicInteger[LEVELS];

    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejectedQueueFull = new LongAdder();
    private final LongAdder rejectedConsumerFull = new LongAdder();
    private final LongAdder evicted = new LongAdder();
    private final AtomicBoolean rebasing = new AtomicBoolean();

    /**
     * Sink for DROP_OLDEST evictions. Volatile because the engine installs it at startup and tests
     * replace it; the offer path reads it once per overflow.
     */
    private volatile @Nullable EvictionListener evictionListener;

    private volatile boolean closed;

    /**
     * Creates a queue from the scheduler configuration.
     *
     * @param settings scheduler settings; the queue uses the global capacity, the scheduling options
     *                 and the default consumer policy
     * @param clock    monotonic clock used for aging and snapshot timestamps
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if the configured capacity is not positive
     */
    public BoundedComputeQueue(final ComputeSettings settings, final ComputeClock clock) {
        this(
            settings.queue().capacity(),
            settings.scheduling(),
            settings.consumers().defaultPolicy(),
            clock
        );
    }

    /**
     * Creates a queue with explicit values.
     *
     * @param capacity      global bound on queued tasks, at least {@code 1}
     * @param scheduling    dispatch strategy, aging and work stealing options
     * @param defaultPolicy policy used for consumers that were never registered explicitly
     * @param clock         monotonic clock used for aging and snapshot timestamps
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if the capacity is not positive
     */
    public BoundedComputeQueue(
        final int capacity,
        final ComputeSettings.Scheduling scheduling,
        final ConsumerPolicy defaultPolicy,
        final ComputeClock clock
    ) {
        this.capacity = Preconditions.checkPositive(capacity, "capacity");
        Objects.requireNonNull(scheduling, "scheduling");
        this.defaultPolicy = Objects.requireNonNull(defaultPolicy, "defaultPolicy");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.strategy = scheduling.strategy();
        this.priorityAging = scheduling.priorityAging();
        this.agingIntervalNanos = toNanosSaturated(scheduling.agingInterval());
        this.maxAgingBoost = scheduling.maxAgingBoost();
        this.workStealing = scheduling.workStealing();
        for (int level = 0; level < LEVELS; level++) {
            this.levelCounts[level] = new AtomicInteger();
        }
    }

    @Override
    public OfferResult offer(final QueueEntry entry) {
        Objects.requireNonNull(entry, "entry");
        int retirementRaces = 0;
        while (true) {
            if (this.closed) {
                return OfferResult.CLOSED;
            }
            final ConsumerState state = this.stateFor(entry.consumerId());
            final ConsumerPolicy policy = state.policy;
            final int consumerBound = policy.maxQueuedTasks();
            if (consumerBound >= 0 && state.queued.get() >= consumerBound) {
                final OfferResult refusal = this.handleOverflow(state, policy, OfferResult.CONSUMER_QUEUE_FULL);
                if (refusal != null) {
                    return refusal;
                }
                continue;
            }
            if (!this.reserveGlobal()) {
                final OfferResult refusal = this.handleOverflow(state, policy, OfferResult.QUEUE_FULL);
                if (refusal != null) {
                    return refusal;
                }
                continue;
            }
            final int previousDepth = this.reserveConsumer(state, consumerBound);
            if (previousDepth < 0) {
                this.queued.decrementAndGet();
                final OfferResult refusal = this.handleOverflow(state, policy, OfferResult.CONSUMER_QUEUE_FULL);
                if (refusal != null) {
                    return refusal;
                }
                continue;
            }
            if (previousDepth == 0) {
                // the consumer just became runnable again; align its clock so a long idle period
                // cannot turn into an unbounded burst of dispatch turns.
                this.alignVirtualService(state);
            }
            final Node node = new Node(entry, state);
            // putIfAbsent is not needed: sequences are monotone, so the comparator cannot collide.
            state.bucket(node.level).put(node, node);
            this.levelCounts[node.level].incrementAndGet();
            if (state.retired && node.claim()) {
                // the consumer was unregistered while we were inserting; drop the entry and retry
                // against whatever registration exists now.
                this.unlink(node);
                retirementRaces++;
                if (retirementRaces > MAX_RETIREMENT_RACES) {
                    this.rejectedQueueFull.increment();
                    return OfferResult.QUEUE_FULL;
                }
                continue;
            }
            this.accepted.increment();
            return OfferResult.ACCEPTED;
        }
    }

    @Override
    public void setEvictionListener(final EvictionListener listener) {
        this.evictionListener = Objects.requireNonNull(listener, "listener");
    }

    @Override
    public @Nullable QueueEntry poll(final ConsumerAdmission admission) {
        return this.poll(Lane.PLATFORM, ALL_LEVELS, admission);
    }

    @Override
    public @Nullable QueueEntry poll(final Lane lane, final int levelMask, final ConsumerAdmission admission) {
        Objects.requireNonNull(lane, "lane");
        Objects.requireNonNull(admission, "admission");
        while (true) {
            final long now = this.clock.nanoTime();
            final Node candidate = this.select(admission, now, levelMask);
            if (candidate == null) {
                return null;
            }
            if (!candidate.claim()) {
                // lost the race to another consumer; its removal is progress, so select again
                continue;
            }
            this.unlink(candidate);
            this.charge(candidate.owner);
            return candidate.entry();
        }
    }

    @Override
    public @Nullable QueueEntry steal(final int thiefIndex, final ConsumerAdmission admission) {
        return this.steal(thiefIndex, Lane.PLATFORM, ALL_LEVELS, admission);
    }

    @Override
    public @Nullable QueueEntry steal(
        final int thiefIndex,
        final Lane lane,
        final int levelMask,
        final ConsumerAdmission admission
    ) {
        Objects.requireNonNull(lane, "lane");
        Objects.requireNonNull(admission, "admission");
        if (!this.workStealing) {
            return null;
        }
        final ConsumerState[] snapshot = this.streams;
        final int length = snapshot.length;
        if (length == 0) {
            return null;
        }
        // start at a different victim for every thief so idle workers do not all hammer the same
        // stream; the rotation is stable because it only depends on the index.
        final int start = Math.floorMod(thiefIndex, length);
        for (int offset = 0; offset < length; offset++) {
            final ConsumerState state = snapshot[(start + offset) % length];
            if (state.queued.get() == 0 || !admission.admits(state.id)) {
                continue;
            }
            final Node candidate = this.nextFrom(state, this.clock.nanoTime(), levelMask);
            if (candidate == null || !candidate.claim()) {
                continue;
            }
            this.unlink(candidate);
            this.charge(state);
            return candidate.entry();
        }
        return null;
    }

    @Override
    public int queuedAtLevel(final int level) {
        Preconditions.checkAtLeast(level, 0, "level");
        if (level >= LEVELS) {
            throw new IllegalArgumentException("level must be below " + LEVELS + ", got " + level);
        }
        return Math.max(0, this.levelCounts[level].get());
    }

    @Override
    public int queuedInLane(final Lane lane, final int levelMask) {
        Objects.requireNonNull(lane, "lane");
        int total = 0;
        for (int level = 0; level < LEVELS; level++) {
            if ((levelMask & (1 << level)) != 0) {
                total += Math.max(0, this.levelCounts[level].get());
            }
        }
        return total;
    }

    @Override
    public long oldestWaitNanos(final int level, final long nowNanos) {
        Preconditions.checkAtLeast(level, 0, "level");
        if (level >= LEVELS) {
            throw new IllegalArgumentException("level must be below " + LEVELS + ", got " + level);
        }
        long oldest = Long.MAX_VALUE;
        for (final ConsumerState state : this.streams) {
            final Node head = state.headOf(level);
            if (head != null) {
                oldest = Math.min(oldest, head.submittedAtNanos());
            }
        }
        return oldest == Long.MAX_VALUE ? 0L : Math.max(0L, nowNanos - oldest);
    }

    @Override
    public @Nullable QueueEntry peek() {
        final Node candidate = this.select(ALWAYS_ADMIT, this.clock.nanoTime());
        return candidate == null ? null : candidate.entry();
    }

    @Override
    public boolean remove(final QueueEntry entry) {
        Objects.requireNonNull(entry, "entry");
        final ConsumerState state = this.consumers.get(entry.consumerId());
        if (state == null) {
            return false;
        }
        final Node probe = new Node(entry.sequence(), entry.taskId());
        final Node candidate = state.bucket(entry.priority().level()).ceilingKey(probe);
        if (candidate == null || candidate.sequence != entry.sequence() || candidate.taskId != entry.taskId()) {
            return false;
        }
        if (!candidate.claim()) {
            return false;
        }
        this.unlink(candidate);
        return true;
    }

    @Override
    public int size() {
        return this.queued.get();
    }

    @Override
    public int size(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final ConsumerState state = this.consumers.get(consumerId);
        return state == null ? 0 : state.queued.get();
    }

    @Override
    public int capacity() {
        return this.capacity;
    }

    @Override
    public boolean isEmpty() {
        return this.queued.get() == 0;
    }

    @Override
    public int drainTo(final Collection<? super QueueEntry> target, final int maxEntries) {
        Objects.requireNonNull(target, "target");
        Preconditions.checkPositive(maxEntries, "maxEntries");
        int drained = 0;
        while (drained < maxEntries) {
            final long now = this.clock.nanoTime();
            final Node candidate = this.selectByPriority(ALWAYS_ADMIT, now);
            if (candidate == null) {
                break;
            }
            if (!candidate.claim()) {
                continue;
            }
            this.unlink(candidate);
            target.add(candidate.entry());
            drained++;
        }
        return drained;
    }

    @Override
    public boolean hasExpired(final long nowNanos) {
        for (final ConsumerState state : this.streams) {
            if (state.queued.get() == 0) {
                continue;
            }
            final ConsumerPolicy policy = state.policy;
            if (policy.maxQueueWait().isZero()) {
                continue;
            }
            final Node oldest = this.oldestHead(state);
            if (oldest != null && isExpired(oldest, nowNanos, toNanosSaturated(policy.maxQueueWait()))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int drainExpired(final long nowNanos, final Collection<? super QueueEntry> target) {
        Objects.requireNonNull(target, "target");
        final ConsumerState[] snapshot = this.streams;
        final List<Node> expired = new ArrayList<>();
        for (final ConsumerState state : snapshot) {
            if (state.queued.get() == 0) {
                continue;
            }
            final ConsumerPolicy policy = state.policy;
            if (policy.maxQueueWait().isZero()) {
                continue;
            }
            final long limitNanos = toNanosSaturated(policy.maxQueueWait());
            for (int level = 0; level < LEVELS; level++) {
                final ConcurrentSkipListMap<Node, Node> bucket = state.bucket(level);
                Node node = bucket.ceilingKey(LOOKUP_MIN);
                // every bucket is ordered by sequence and the clock advances with the sequence, so
                // the expired entries are a prefix: stop at the first one that is still alive. the
                // walk costs one extra look per bucket, not one per queued entry.
                while (node != null && isExpired(node, nowNanos, limitNanos)) {
                    expired.add(node);
                    node = bucket.higherKey(node);
                }
            }
        }
        if (expired.isEmpty()) {
            return 0;
        }
        expired.sort((left, right) -> this.compareDispatchOrder(left, right, nowNanos));
        int moved = 0;
        for (final Node node : expired) {
            if (!node.claim()) {
                // a worker or a cancel got it first; it is accounted for there, never twice here
                continue;
            }
            this.unlink(node);
            target.add(node.entry());
            moved++;
        }
        return moved;
    }

    @Override
    public Collection<QueueEntry> clear() {
        final List<QueueEntry> removed = new ArrayList<>(this.queued.get());
        while (true) {
            final long now = this.clock.nanoTime();
            final Node candidate = this.select(ALWAYS_ADMIT, now);
            if (candidate == null) {
                break;
            }
            if (!candidate.claim()) {
                continue;
            }
            this.unlink(candidate);
            // clear is the bulk form of poll, so it follows the same dispatch order, which includes
            // charging the fairness turn of the consumer it just took an entry from.
            this.charge(candidate.owner);
            removed.add(candidate.entry());
        }
        return removed;
    }

    @Override
    public QueueSnapshot snapshot() {
        final long now = this.clock.nanoTime();
        final ConsumerState[] snapshot = this.streams;
        final Map<String, Integer> perConsumer = new HashMap<>();
        long oldestSubmitted = Long.MAX_VALUE;
        for (final ConsumerState state : snapshot) {
            final int depth = state.queued.get();
            if (depth > 0) {
                perConsumer.put(state.id, depth);
            }
            final Node oldest = this.oldestHead(state);
            if (oldest != null) {
                oldestSubmitted = Math.min(oldestSubmitted, oldest.submittedAtNanos());
            }
        }
        final long oldestWait = oldestSubmitted == Long.MAX_VALUE ? 0L : Math.max(0L, now - oldestSubmitted);
        return new QueueSnapshot(
            now,
            this.queued.get(),
            this.capacity,
            perConsumer,
            this.accepted.sum(),
            this.rejectedQueueFull.sum(),
            this.rejectedConsumerFull.sum(),
            this.evicted.sum(),
            oldestWait
        );
    }

    @Override
    public void registerConsumer(final String consumerId, final ConsumerPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Preconditions.requireNonBlank(consumerId, "consumerId");
        this.register(consumerId, policy);
    }

    @Override
    public void updatePolicy(final String consumerId, final ConsumerPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Preconditions.requireNonBlank(consumerId, "consumerId");
        this.register(consumerId, policy);
    }

    @Override
    public Collection<QueueEntry> unregisterConsumer(final String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        final ConsumerState state;
        synchronized (this.topologyLock) {
            state = this.consumers.remove(consumerId);
            if (state == null) {
                return List.of();
            }
            state.retired = true;
            this.detach(state);
        }
        final List<QueueEntry> removed = new ArrayList<>(Math.max(0, state.queued.get()));
        while (true) {
            final long now = this.clock.nanoTime();
            final Node candidate = this.nextFrom(state, now);
            if (candidate == null) {
                break;
            }
            if (!candidate.claim()) {
                continue;
            }
            this.unlink(candidate);
            removed.add(candidate.entry());
        }
        return removed;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    /**
     * Reserves one global slot.
     *
     * @return {@code true} if a slot was taken, {@code false} if the global bound is reached
     */
    private boolean reserveGlobal() {
        while (true) {
            final int current = this.queued.get();
            if (current >= this.capacity) {
                return false;
            }
            if (this.queued.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Reserves one consumer slot.
     *
     * @param state consumer to reserve for
     * @param bound per consumer bound, or {@code -1} for no bound
     * @return the depth before the reservation, or {@code -1} if the bound is reached
     */
    private int reserveConsumer(final ConsumerState state, final int bound) {
        while (true) {
            final int current = state.queued.get();
            if (bound >= 0 && current >= bound) {
                return -1;
            }
            if (state.queued.compareAndSet(current, current + 1)) {
                return current;
            }
        }
    }

    /**
     * Applies the consumer's overflow policy to a full bound.
     *
     * @param state   consumer that hit the bound
     * @param policy  policy in force for that consumer
     * @param refusal result to report if nothing can be evicted
     * @return {@code null} if an entry was evicted and the offer can be retried, otherwise the
     *     refusal result, already counted
     * @throws IllegalStateException if {@link OverflowPolicy#DROP_OLDEST} needs to evict an entry but
     *     no eviction listener is installed
     */
    private @Nullable OfferResult handleOverflow(
        final ConsumerState state,
        final ConsumerPolicy policy,
        final OfferResult refusal
    ) {
        if (policy.overflowPolicy() == OverflowPolicy.DROP_OLDEST) {
            final EvictionListener listener = this.evictionListener;
            if (listener == null) {
                if (this.oldestHead(state) != null) {
                    // dropping the entry without reporting it would leave its future pending forever.
                    throw new IllegalStateException(
                        "consumer " + state.id + " uses DROP_OLDEST but no eviction listener is installed"
                    );
                }
            } else {
                final QueueEntry evictedEntry = this.evictOldest(state);
                if (evictedEntry != null) {
                    // the node is already unlinked and both depth counters are updated, so the
                    // listener can inspect the queue; it runs on this thread, before the offer
                    // returns, and outside every lock.
                    listener.onEvicted(evictedEntry);
                    return null;
                }
            }
        }
        if (refusal == OfferResult.CONSUMER_QUEUE_FULL) {
            this.rejectedConsumerFull.increment();
        } else {
            this.rejectedQueueFull.increment();
        }
        return refusal;
    }

    /**
     * Evicts the oldest queued entry of one consumer.
     *
     * <p>Claiming the node is atomic, so two concurrent offers can never evict and report the same
     * entry; the loser recomputes the head and either evicts another entry or finds nothing left.
     *
     * @param state consumer to evict from
     * @return the evicted entry, already unlinked, or {@code null} if the stream is empty
     */
    private @Nullable QueueEntry evictOldest(final ConsumerState state) {
        while (true) {
            final Node oldest = this.oldestHead(state);
            if (oldest == null) {
                return null;
            }
            if (!oldest.claim()) {
                // somebody else took it; every failed claim means the stream shrank
                continue;
            }
            this.unlink(oldest);
            this.evicted.increment();
            return oldest.entry();
        }
    }

    /**
     * Removes one node from its bucket and from both depth counters.
     *
     * @param node claimed node
     */
    private void unlink(final Node node) {
        final ConsumerState owner = node.owner();
        owner.bucket(node.level).remove(node);
        owner.queued.decrementAndGet();
        this.levelCounts[node.level].decrementAndGet();
        this.queued.decrementAndGet();
    }

    /**
     * Returns the best candidate of one strategy aware selection pass.
     *
     * @param admission admission check, applied per consumer
     * @param nowNanos  current clock reading
     * @return the best node, or {@code null} if nothing is admitted right now
     */
    private @Nullable Node select(final ConsumerAdmission admission, final long nowNanos) {
        return this.select(admission, nowNanos, ALL_LEVELS);
    }

    /**
     * Returns the best candidate of one strategy aware selection pass restricted to a level mask.
     *
     * @param admission admission check, applied per consumer
     * @param nowNanos  current clock reading
     * @param levelMask bit set of priority levels this pass may pick
     * @return the best node, or {@code null} if nothing is admitted right now
     */
    private @Nullable Node select(final ConsumerAdmission admission, final long nowNanos, final int levelMask) {
        final ConsumerState[] snapshot = this.streams;
        Node best = null;
        int bestLevel = Integer.MIN_VALUE;
        long bestService = 0L;
        for (final ConsumerState state : snapshot) {
            if (state.queued.get() == 0 || !admission.admits(state.id)) {
                continue;
            }
            final Node head = this.nextFrom(state, nowNanos, levelMask);
            if (head == null) {
                continue;
            }
            final int level = this.strategy == ComputeSettings.Scheduling.Strategy.FIFO
                ? 0
                : this.effectiveLevel(head, nowNanos);
            final long service = this.strategy == ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR
                ? state.virtualService.get()
                : 0L;
            if (best == null
                || level > bestLevel
                || (level == bestLevel && service < bestService)
                || (level == bestLevel && service == bestService && head.sequence < best.sequence)) {
                best = head;
                bestLevel = level;
                bestService = service;
            }
        }
        return best;
    }

    /**
     * Returns the best candidate of one priority only selection pass, ignoring the strategy and the
     * virtual service time.
     *
     * <p>Used by {@link #drainTo(Collection, int)}, whose contract is "oldest and highest priority
     * first" regardless of how the scheduler normally dispatches.
     *
     * @param admission admission check, applied per consumer
     * @param nowNanos  current clock reading
     * @return the best node, or {@code null} if nothing is admitted right now
     */
    private @Nullable Node selectByPriority(final ConsumerAdmission admission, final long nowNanos) {
        final ConsumerState[] snapshot = this.streams;
        Node best = null;
        int bestLevel = Integer.MIN_VALUE;
        for (final ConsumerState state : snapshot) {
            if (state.queued.get() == 0 || !admission.admits(state.id)) {
                continue;
            }
            final Node head = this.bestHead(state, nowNanos);
            if (head == null) {
                continue;
            }
            final int level = this.effectiveLevel(head, nowNanos);
            if (best == null || level > bestLevel || (level == bestLevel && head.sequence < best.sequence)) {
                best = head;
                bestLevel = level;
            }
        }
        return best;
    }

    /**
     * Returns the next entry of one consumer in dispatch order.
     *
     * @param state    consumer to inspect
     * @param nowNanos current clock reading
     * @return the entry that would be dispatched first, or {@code null} if the stream is empty
     */
    private @Nullable Node nextFrom(final ConsumerState state, final long nowNanos) {
        return this.nextFrom(state, nowNanos, ALL_LEVELS);
    }

    /**
     * Returns the next entry of one consumer in dispatch order, restricted to a level mask.
     *
     * @param state     consumer to inspect
     * @param nowNanos  current clock reading
     * @param levelMask bit set of priority levels this lane may pick
     * @return the entry that would be dispatched first, or {@code null} if nothing is eligible
     */
    private @Nullable Node nextFrom(final ConsumerState state, final long nowNanos, final int levelMask) {
        if (this.strategy == ComputeSettings.Scheduling.Strategy.FIFO) {
            return this.oldestHead(state, levelMask);
        }
        return this.bestHead(state, nowNanos, levelMask);
    }

    /**
     * Returns the highest effective head of one consumer.
     *
     * @param state    consumer to inspect
     * @param nowNanos current clock reading
     * @return the best bucket head, or {@code null} if the stream is empty
     */
    private @Nullable Node bestHead(final ConsumerState state, final long nowNanos) {
        return this.bestHead(state, nowNanos, ALL_LEVELS);
    }

    /**
     * Returns the highest effective head of one consumer among the levels a lane may pick.
     *
     * @param state     consumer to inspect
     * @param nowNanos  current clock reading
     * @param levelMask bit set of priority levels this pass may pick
     * @return the best eligible bucket head, or {@code null} if nothing is eligible
     */
    private @Nullable Node bestHead(final ConsumerState state, final long nowNanos, final int levelMask) {
        Node best = null;
        int bestLevel = Integer.MIN_VALUE;
        for (int level = LEVELS - 1; level >= 0; level--) {
            if ((levelMask & (1 << level)) == 0) {
                continue;
            }
            final Node head = state.headOf(level);
            if (head == null) {
                continue;
            }
            final int effective = this.effectiveLevel(head, nowNanos);
            if (best == null || effective > bestLevel || (effective == bestLevel && head.sequence < best.sequence)) {
                best = head;
                bestLevel = effective;
            }
        }
        return best;
    }

    /**
     * Returns the oldest entry of one consumer across all buckets.
     *
     * @param state consumer to inspect
     * @return the entry with the lowest submission sequence, or {@code null} if the stream is empty
     */
    private @Nullable Node oldestHead(final ConsumerState state) {
        return this.oldestHead(state, ALL_LEVELS);
    }

    /**
     * Returns the oldest entry of one consumer among the levels a lane may pick.
     *
     * @param state     consumer to inspect
     * @param levelMask bit set of priority levels this pass may pick
     * @return the oldest eligible head, or {@code null} if nothing is eligible
     */
    private @Nullable Node oldestHead(final ConsumerState state, final int levelMask) {
        Node oldest = null;
        for (int level = 0; level < LEVELS; level++) {
            if ((levelMask & (1 << level)) == 0) {
                continue;
            }
            final Node head = state.headOf(level);
            if (head != null && (oldest == null || head.sequence < oldest.sequence)) {
                oldest = head;
            }
        }
        return oldest;
    }

    /**
     * Computes the effective priority of an entry at the given instant.
     *
     * @param node     queued node
     * @param nowNanos current clock reading
     * @return the submitted level plus one boost per aging interval, clamped to the top level
     */
    private int effectiveLevel(final Node node, final long nowNanos) {
        final int base = node.level;
        if (!this.priorityAging) {
            return base;
        }
        final long waited = nowNanos - node.submittedAtNanos();
        if (waited <= 0L) {
            return base;
        }
        final long steps = Math.min(waited / this.agingIntervalNanos, this.maxAgingBoost);
        final long boosted = base + steps;
        return (int) Math.min(ComputePriority.CRITICAL.level(), boosted);
    }

    /**
     * Checks whether an entry outlived a queue wait limit.
     *
     * @param node       queued node
     * @param nowNanos   current clock reading
     * @param limitNanos wait limit in nanoseconds, strictly positive
     * @return {@code true} when the entry reached or passed the limit
     */
    private static boolean isExpired(final Node node, final long nowNanos, final long limitNanos) {
        return nowNanos - node.submittedAtNanos() >= limitNanos;
    }

    /**
     * Orders two nodes the way a sweep hands them back: highest effective priority, then least served
     * consumer, then submission order.
     *
     * <p>Expiry is not a dispatch, so this never charges virtual service time; it only reads it to
     * keep the order consistent with the queue's own ordering inputs.
     *
     * @param left     first node
     * @param right    second node
     * @param nowNanos instant used to compute aging
     * @return negative when {@code left} comes first
     */
    private int compareDispatchOrder(final Node left, final Node right, final long nowNanos) {
        final int leftLevel = this.effectiveLevel(left, nowNanos);
        final int rightLevel = this.effectiveLevel(right, nowNanos);
        if (leftLevel != rightLevel) {
            return Integer.compare(rightLevel, leftLevel);
        }
        final long leftService = left.owner().virtualService.get();
        final long rightService = right.owner().virtualService.get();
        if (leftService != rightService) {
            return Long.compare(leftService, rightService);
        }
        return Long.compare(left.sequence, right.sequence);
    }

    /**
     * Charges one dispatch turn of virtual service time and rebases the counters when they grow too
     * large.
     *
     * @param state consumer that was dispatched
     */
    private void charge(final ConsumerState state) {
        final double weight = state.policy.fairShareWeight();
        final long charge = Math.max(1L, Math.min(MAX_DISPATCH_CHARGE, Math.round(FAIRNESS_SCALE / weight)));
        final long service = state.virtualService.addAndGet(charge);
        if (service > REBASE_THRESHOLD && this.rebasing.compareAndSet(false, true)) {
            try {
                this.rebaseVirtualTime();
            } finally {
                this.rebasing.set(false);
            }
        }
    }

    /**
     * Subtracts the smallest virtual service value from every consumer.
     *
     * <p>Only differences matter, so this keeps the counters in range without changing the order.
     * Runs at most once per reboot threshold and is guarded by {@link #rebasing}.
     */
    private void rebaseVirtualTime() {
        final ConsumerState[] snapshot = this.streams;
        long minimum = Long.MAX_VALUE;
        for (final ConsumerState state : snapshot) {
            minimum = Math.min(minimum, state.virtualService.get());
        }
        if (minimum == Long.MAX_VALUE || minimum <= 0L) {
            return;
        }
        for (final ConsumerState state : snapshot) {
            state.virtualService.addAndGet(-minimum);
        }
    }

    /**
     * Aligns the virtual service clock of a consumer that just became runnable again.
     *
     * <p>Without this, a subsystem that was idle for an hour would come back with an hour of unused
     * turns and monopolise the pool until it caught up. Starting from the least served backlogged
     * consumer keeps the catch up bounded by one round, while still giving the returning consumer
     * its normal share immediately.
     *
     * @param state consumer that just received its first queued entry
     */
    private void alignVirtualService(final ConsumerState state) {
        if (this.strategy != ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR) {
            return;
        }
        long minimum = Long.MAX_VALUE;
        for (final ConsumerState other : this.streams) {
            if (other != state && other.queued.get() > 0) {
                minimum = Math.min(minimum, other.virtualService.get());
            }
        }
        if (minimum != Long.MAX_VALUE && state.virtualService.get() < minimum) {
            state.virtualService.set(minimum);
        }
    }

    /**
     * Returns the state of a consumer, registering it with the default policy when it is unknown.
     *
     * @param consumerId consumer to look up
     * @return the state, never {@code null}
     */
    private ConsumerState stateFor(final String consumerId) {
        final ConsumerState existing = this.consumers.get(consumerId);
        if (existing != null) {
            return existing;
        }
        return this.register(consumerId, this.defaultPolicy);
    }

    /**
     * Registers or re-registers a consumer.
     *
     * <p>A single short critical section: consumer topology changes are administrative, they never
     * happen per task, and they must not race with the copy on write snapshot the hot path reads.
     *
     * @param consumerId consumer to register
     * @param policy     policy to store
     * @return the consumer state, never {@code null}
     */
    private ConsumerState register(final String consumerId, final ConsumerPolicy policy) {
        synchronized (this.topologyLock) {
            final ConsumerState existing = this.consumers.get(consumerId);
            if (existing != null) {
                existing.policy = policy;
                return existing;
            }
            final ConsumerState created = new ConsumerState(consumerId, policy);
            this.consumers.put(consumerId, created);
            final ConsumerState[] current = this.streams;
            final ConsumerState[] next = Arrays.copyOf(current, current.length + 1);
            next[current.length] = created;
            this.streams = next;
            return created;
        }
    }

    /**
     * Removes a consumer state from the polling view. Called with {@link #topologyLock} held.
     *
     * @param state consumer to remove
     */
    private void detach(final ConsumerState state) {
        final ConsumerState[] current = this.streams;
        for (int index = 0; index < current.length; index++) {
            if (current[index] != state) {
                continue;
            }
            final ConsumerState[] next = new ConsumerState[current.length - 1];
            System.arraycopy(current, 0, next, 0, index);
            System.arraycopy(current, index + 1, next, index, current.length - index - 1);
            this.streams = next;
            return;
        }
    }

    /**
     * Converts a duration to nanoseconds without blowing up on absurd configuration values.
     *
     * @param duration positive duration
     * @return the duration in nanoseconds, saturated at {@link Long#MAX_VALUE}
     */
    private static long toNanosSaturated(final Duration duration) {
        try {
            return Math.max(1L, duration.toNanos());
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * One queued submission.
     *
     * <p>The node is the key of its bucket, so a bucket insert allocates no key object. The same
     * instance is also the value, which keeps the map entry a single reference on both sides.
     */
    private static final class Node {

        private final long sequence;
        private final long taskId;
        private final int level;
        private final @Nullable QueueEntry entry;
        private final @Nullable ConsumerState owner;
        private final AtomicInteger claimed = new AtomicInteger();

        /**
         * Creates a queued node.
         *
         * @param entry entry being queued
         * @param owner stream that owns it
         */
        Node(final QueueEntry entry, final ConsumerState owner) {
            this.sequence = entry.sequence();
            this.taskId = entry.taskId();
            this.level = entry.priority().level();
            this.entry = entry;
            this.owner = owner;
        }

        /**
         * Creates a lookup node, used for probes and as the lower sentinel of a bucket.
         *
         * @param sequence sequence to compare against
         * @param taskId   task id to break equal sequences
         */
        Node(final long sequence, final long taskId) {
            this.sequence = sequence;
            this.taskId = taskId;
            this.level = 0;
            this.entry = null;
            this.owner = null;
        }

        /**
         * Claims the node for exactly one owner.
         *
         * @return {@code true} if this call won the node
         */
        boolean claim() {
            return this.claimed.compareAndSet(0, 1);
        }

        /**
         * Returns the queued entry.
         *
         * @return the entry, never {@code null} for a real node
         */
        QueueEntry entry() {
            return Objects.requireNonNull(this.entry, "lookup node has no entry");
        }

        /**
         * Returns the owning stream.
         *
         * @return the stream, never {@code null} for a real node
         */
        ConsumerState owner() {
            return Objects.requireNonNull(this.owner, "lookup node has no owner");
        }

        /**
         * Returns the submission timestamp of the queued entry.
         *
         * @return the monotonic submission timestamp
         */
        long submittedAtNanos() {
            return this.entry().submittedAtNanos();
        }
    }

    /**
     * Per consumer stream: four priority buckets, a depth counter and the virtual service time.
     */
    private static final class ConsumerState {

        private final String id;
        private final ConcurrentSkipListMap<Node, Node> critical;
        private final ConcurrentSkipListMap<Node, Node> high;
        private final ConcurrentSkipListMap<Node, Node> normal;
        private final ConcurrentSkipListMap<Node, Node> low;
        private final AtomicInteger queued = new AtomicInteger();
        private final AtomicLong virtualService = new AtomicLong();
        private volatile ConsumerPolicy policy;
        private volatile boolean retired;

        /**
         * Creates a stream.
         *
         * @param id     consumer id
         * @param policy initial policy
         */
        ConsumerState(final String id, final ConsumerPolicy policy) {
            this.id = id;
            this.policy = policy;
            this.critical = new ConcurrentSkipListMap<>(SEQUENCE_ORDER);
            this.high = new ConcurrentSkipListMap<>(SEQUENCE_ORDER);
            this.normal = new ConcurrentSkipListMap<>(SEQUENCE_ORDER);
            this.low = new ConcurrentSkipListMap<>(SEQUENCE_ORDER);
        }

        /**
         * Returns the bucket of one level.
         *
         * @param level priority level, {@code 0} to {@code 3}
         * @return the bucket
         */
        ConcurrentSkipListMap<Node, Node> bucket(final int level) {
            return switch (level) {
                case 3 -> this.critical;
                case 2 -> this.high;
                case 1 -> this.normal;
                default -> this.low;
            };
        }

        /**
         * Returns the oldest entry of one bucket without allocating a map entry.
         *
         * @param level priority level, {@code 0} to {@code 3}
         * @return the head, or {@code null} if that bucket is empty
         */
        @Nullable Node headOf(final int level) {
            return this.bucket(level).ceilingKey(LOOKUP_MIN);
        }
    }
}
