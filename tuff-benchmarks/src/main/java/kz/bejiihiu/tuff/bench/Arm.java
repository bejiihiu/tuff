package kz.bejiihiu.tuff.bench;

import java.util.Locale;

/**
 * One comparison arm of the scheduler methodology.
 *
 * <p>The four arms are fixed by the pre-registered design: {@code A} is Folia's EDF baseline,
 * {@code A2} is the same configuration measured a second time and exists only to calibrate the noise
 * band of the stand, {@code B} is the Kairos port with {@code min == max} (no elasticity) and
 * {@code C} is Kairos with a real ceiling above the floor.
 *
 * <p>Arms differ by exactly one factor each: {@code A} to {@code B} changes the scheduler class,
 * {@code B} to {@code C} turns elasticity on. Nothing else in the run configuration is allowed to
 * vary.
 */
enum Arm {

    /** Folia's earliest-deadline-first scheduler. */
    A("A", "EDF", false, false),

    /** The EDF baseline measured twice; the label is the only difference from {@link #A}. */
    A2("A2", "EDF", false, false),

    /** Kairos with a fixed fund: the EDF port without dynamic growth. */
    B("B", "KAIROS", false, true),

    /** Kairos with an elastic fund: the actual hypothesis. */
    C("C", "KAIROS", true, true);

    private final String label;
    private final String scheduler;
    private final boolean elastic;
    private final boolean kairosOptIn;

    Arm(final String label, final String scheduler, final boolean elastic, final boolean kairosOptIn) {
        this.label = label;
        this.scheduler = scheduler;
        this.elastic = elastic;
        this.kairosOptIn = kairosOptIn;
    }

    /**
     * Returns the label written into artifacts and used by the analyzer.
     *
     * @return {@code A}, {@code A2}, {@code B} or {@code C}
     */
    String label() {
        return this.label;
    }

    /**
     * Returns the value for {@code threaded-regions.scheduler} in {@code config/paper-global.yml}.
     *
     * @return {@code EDF} or {@code KAIROS}
     */
    String scheduler() {
        return this.scheduler;
    }

    /**
     * Returns whether the Kairos fund may grow and shrink.
     *
     * @return {@code true} only for arm {@code C}
     */
    boolean elastic() {
        return this.elastic;
    }

    /**
     * Returns whether this arm has to opt in through {@code kairos-unstable}.
     *
     * @return {@code true} for {@code B} and {@code C}
     */
    boolean kairosOptIn() {
        return this.kairosOptIn;
    }

    /**
     * Parses an arm label.
     *
     * @param raw label, case insensitive
     * @return the arm
     * @throws IllegalArgumentException if the label is unknown
     */
    static Arm parse(final String raw) {
        final String normalized = raw.trim().toUpperCase(Locale.ROOT);
        for (final Arm arm : values()) {
            if (arm.label.equals(normalized)) {
                return arm;
            }
        }
        throw new IllegalArgumentException(
            "unknown arm '" + raw + "'; expected one of A, A2, B, C"
        );
    }
}
