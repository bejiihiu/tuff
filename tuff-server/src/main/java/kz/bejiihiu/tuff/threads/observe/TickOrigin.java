package kz.bejiihiu.tuff.threads.observe;

/**
 * Scheduler that executed a region tick body.
 *
 * <p>The origin is written into every observation so a recording can tell the stock schedulers apart
 * from the experimental one without guessing from thread names.
 */
public enum TickOrigin {

    /**
     * Folia's earliest deadline first scheduler.
     */
    EDF,

    /**
     * Folia's work stealing scheduler.
     */
    WORK_STEALING,

    /**
     * Tuff's own deadline scheduler with an elastic fund.
     */
    KAIROS
}
