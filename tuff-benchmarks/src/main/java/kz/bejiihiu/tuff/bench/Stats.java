package kz.bejiihiu.tuff.bench;

import java.util.Arrays;
import java.util.Random;

/**
 * Small, dependency free statistics used by the analyzer.
 *
 * <p>The unit of analysis is a run, never a tick: ticks inside one run are autocorrelated, so a
 * confidence interval built over them would claim a precision the stand does not have. Every function
 * here therefore takes one value per run.
 *
 * <p>Bootstrap resampling is deterministic: the same inputs and the same seed produce the same
 * interval, so a report can be recomputed from the stored summaries.
 */
final class Stats {

    private Stats() {
    }

    /**
     * Returns the arithmetic mean.
     *
     * @param values non empty values
     * @return the mean, or {@code NaN} for an empty input
     */
    static double mean(final double[] values) {
        if (values.length == 0) {
            return Double.NaN;
        }
        double sum = 0.0d;
        for (final double value : values) {
            sum += value;
        }
        return sum / values.length;
    }

    /**
     * Returns the median.
     *
     * @param values values, in any order
     * @return the median, or {@code NaN} for an empty input
     */
    static double median(final double[] values) {
        if (values.length == 0) {
            return Double.NaN;
        }
        final double[] sorted = values.clone();
        Arrays.sort(sorted);
        final int middle = sorted.length / 2;
        return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2.0d : sorted[middle];
    }

    /**
     * Returns the sample standard deviation.
     *
     * @param values at least two values
     * @return the sample standard deviation, or {@code 0} for fewer than two values
     */
    static double standardDeviation(final double[] values) {
        if (values.length < 2) {
            return 0.0d;
        }
        final double mean = mean(values);
        double squareSum = 0.0d;
        for (final double value : values) {
            final double delta = value - mean;
            squareSum += delta * delta;
        }
        return Math.sqrt(squareSum / (values.length - 1));
    }

    /**
     * Returns a percentile using the nearest-rank definition.
     *
     * @param values values, in any order
     * @param fraction fraction in {@code [0, 1]}
     * @return the percentile, or {@code NaN} for an empty input
     */
    static double percentile(final double[] values, final double fraction) {
        if (values.length == 0) {
            return Double.NaN;
        }
        final double[] sorted = values.clone();
        Arrays.sort(sorted);
        final int rank = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.clamp(rank, 0, sorted.length - 1)];
    }

    /**
     * Returns a percentile of already sorted values.
     *
     * @param sorted ascending values
     * @param fraction fraction in {@code [0, 1]}
     * @return the percentile, or {@code NaN} for an empty input
     */
    static double percentile(final long[] sorted, final double fraction) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        final int rank = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.clamp(rank, 0, sorted.length - 1)];
    }

    /**
     * Bootstraps the mean of one arm.
     *
     * @param values per-run values
     * @param resamples how many resamples to draw
     * @param seed deterministic seed
     * @return the point estimate with its 95% percentile interval
     */
    static Interval bootstrapMean(final double[] values, final int resamples, final long seed) {
        if (values.length == 0) {
            return new Interval(Double.NaN, Double.NaN, Double.NaN);
        }
        final Random random = new Random(seed);
        final double[] means = new double[resamples];
        for (int sample = 0; sample < resamples; ++sample) {
            double sum = 0.0d;
            for (int draw = 0; draw < values.length; ++draw) {
                sum += values[random.nextInt(values.length)];
            }
            means[sample] = sum / values.length;
        }
        return new Interval(percentile(means, 0.025d), mean(values), percentile(means, 0.975d));
    }

    /**
     * Bootstraps the difference of two independent arm means, {@code left - right}.
     *
     * @param left      per-run values of the arm under test
     * @param right     per-run values of the baseline
     * @param resamples how many resamples to draw
     * @param seed      deterministic seed
     * @return the observed difference with its 95% percentile interval
     */
    static Interval bootstrapDifference(
        final double[] left,
        final double[] right,
        final int resamples,
        final long seed
    ) {
        if (left.length == 0 || right.length == 0) {
            return new Interval(Double.NaN, Double.NaN, Double.NaN);
        }
        final Random random = new Random(seed);
        final double[] differences = new double[resamples];
        for (int sample = 0; sample < resamples; ++sample) {
            double leftSum = 0.0d;
            for (int draw = 0; draw < left.length; ++draw) {
                leftSum += left[random.nextInt(left.length)];
            }
            double rightSum = 0.0d;
            for (int draw = 0; draw < right.length; ++draw) {
                rightSum += right[random.nextInt(right.length)];
            }
            differences[sample] = leftSum / left.length - rightSum / right.length;
        }
        return new Interval(
            percentile(differences, 0.025d),
            mean(left) - mean(right),
            percentile(differences, 0.975d)
        );
    }

    /**
     * A point estimate and its interval.
     *
     * @param low  lower bound of the 95% interval
     * @param mean observed value
     * @param high upper bound of the 95% interval
     */
    record Interval(double low, double mean, double high) {

        /**
         * Returns whether the interval is usable.
         *
         * @return {@code true} when all three numbers are finite
         */
        boolean finite() {
            return Double.isFinite(this.low) && Double.isFinite(this.mean) && Double.isFinite(this.high);
        }
    }
}
