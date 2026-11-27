package kz.bejiihiu.tuff.threads.kairos;

import java.util.Objects;
import kz.bejiihiu.tuff.threads.idle.IdleWorkSource;
import org.jspecify.annotations.Nullable;

/**
 * The runtime seams the fund uses, carried together from the thread bootstrap to the scheduler.
 *
 * <p>They are one value because they have one owner and one lifetime: all three exist only while the
 * thread layer runs, and all three must disappear together when it stops. Keeping them apart would let
 * a scheduler start with a budget from one runtime and a supervisor from another.
 *
 * @param budget      source of the fund ceiling, or {@code null} for the core count fallback
 * @param supervisors source of the supervisor thread, never {@code null}
 * @param idle        source of idle-safe jobs, or {@code null} when nothing may run on an idle runner
 */
public record KairosServices(
    @Nullable KairosBudgetPort budget,
    KairosSupervisorSource supervisors,
    @Nullable IdleWorkSource idle
) {

    /**
     * Validates the services.
     *
     * @throws NullPointerException if {@code supervisors} is {@code null}
     */
    public KairosServices {
        Objects.requireNonNull(supervisors, "supervisors");
    }

    /**
     * Creates the services of a layer that has no idle-safe work to offer.
     *
     * @param budget      source of the fund ceiling, or {@code null}
     * @param supervisors source of the supervisor thread, never {@code null}
     */
    public KairosServices(final @Nullable KairosBudgetPort budget, final KairosSupervisorSource supervisors) {
        this(budget, supervisors, null);
    }

    /**
     * Returns the services of a process without a running thread layer.
     *
     * @return no budget, a supervisor source that always falls back to a plain daemon and no idle work
     */
    public static KairosServices none() {
        return new KairosServices(null, KairosSupervisorSource.plain(), null);
    }
}
