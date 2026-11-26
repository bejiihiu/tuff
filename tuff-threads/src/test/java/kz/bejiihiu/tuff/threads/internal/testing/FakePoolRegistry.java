package kz.bejiihiu.tuff.threads.internal.testing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;

/**
 * In memory pool registry for tests.
 *
 * <p>Mirrors the documented behaviour that matters to the factory: L0 is rejected, ids are unique but
 * replaceable, and a kind change fails. {@link #force(PoolSpec)} bypasses the L0 check so tests can
 * prove that the factory itself refuses an out of scope pool.
 */
public final class FakePoolRegistry implements ExecutionPoolRegistry {

    private final ConcurrentHashMap<PoolId, PoolSpec> specs = new ConcurrentHashMap<>();

    @Override
    public PoolSpec register(final PoolSpec spec) {
        Objects.requireNonNull(spec, "spec");
        if (spec.coverage() == CoverageLevel.L0) {
            throw new IllegalArgumentException("L0 pools have no registry entry");
        }
        final PoolSpec previous = this.specs.get(spec.id());
        if (previous != null && previous.kind() != spec.kind()) {
            throw new IllegalStateException("pool '" + spec.id() + "' already has kind " + previous.kind());
        }
        this.specs.put(spec.id(), spec);
        return spec;
    }

    /**
     * Stores a spec without any validation, for tests that need an impossible registry state.
     *
     * @param spec spec to store
     */
    public void force(final PoolSpec spec) {
        this.specs.put(spec.id(), spec);
    }

    @Override
    public boolean unregister(final PoolId id) {
        Objects.requireNonNull(id, "id");
        return this.specs.remove(id) != null;
    }

    @Override
    public Optional<PoolSpec> spec(final PoolId id) {
        Objects.requireNonNull(id, "id");
        return Optional.ofNullable(this.specs.get(id));
    }

    @Override
    public boolean contains(final PoolId id) {
        Objects.requireNonNull(id, "id");
        return this.specs.containsKey(id);
    }

    @Override
    public Set<PoolId> ids() {
        final List<PoolId> sorted = new ArrayList<>(this.specs.keySet());
        Collections.sort(sorted);
        return Collections.unmodifiableSet(new LinkedHashSet<>(sorted));
    }

    @Override
    public List<PoolSpec> specs() {
        final List<PoolSpec> sorted = new ArrayList<>(this.specs.values());
        sorted.sort(Comparator.comparing(PoolSpec::id));
        return List.copyOf(sorted);
    }

    @Override
    public List<PoolSpec> specs(final PoolKind kind) {
        Objects.requireNonNull(kind, "kind");
        return this.specs().stream().filter(spec -> spec.kind() == kind).toList();
    }
}
