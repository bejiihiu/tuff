package kz.bejiihiu.tuff.threads.kairos;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The two runtime seams the fund uses, carried together from the thread bootstrap to the scheduler.
 *
 * <p>They are one value because they have one owner and one lifetime: both exist only while the thread
 * layer runs, and both must disappear together when it stops. Keeping them apart would let a scheduler
 * start with a budget from one runtime and a supervisor from another.
 *
 * @param budget      source of the fund ceiling, or {@code null} for the core count fallback
 * @param supervisors source of the supervisor thread, never {@code null}
 */
public record KairosServices(@Nullable KairosBudgetPort budget, KairosSupervisorSource supervisors) {

    /**
     * Validates the services.
     *
     * @throws NullPointerException if {@code supervisors} is {@code null}
     */
    public KairosServices {
        Objects.requireNonNull(supervisors, "supervisors");
    }

    /**
     * Returns the services of a process without a running thread layer.
     *
     * @return no budget and a supervisor source that always falls back to a plain daemon
     */
    public static KairosServices none() {
        return new KairosServices(null, KairosSupervisorSource.plain());
    }
}
