package kz.bejiihiu.tuff.compute.internal.spi;

import java.util.Objects;

/**
 * A budget the caller resolved itself.
 *
 * @param processors processors the compute engine may use, at least {@code 1}
 * @param source     short label for the origin of the number
 */
record FixedComputeBudgetPort(int processors, String source) implements ComputeBudgetPort {

    FixedComputeBudgetPort {
        if (processors < 1) {
            throw new IllegalArgumentException("processors must be at least 1, got " + processors);
        }
        Objects.requireNonNull(source, "source");
        if (source.isBlank()) {
            throw new IllegalArgumentException("source must not be blank");
        }
    }
}
