/**
 * Counters, gauges and JFR integration.
 *
 * <p>The metrics component is passive: the engine and the worker pool report events through
 * {@link kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector}, and readers get immutable
 * snapshots back. No scheduling decision ever depends on a metric value, which keeps the hot path
 * free of feedback loops and makes the numbers safe to disable.
 *
 * <p>JFR events live behind {@link kz.bejiihiu.tuff.compute.internal.metrics.JfrRecorder} so the
 * recording classes stay optional and the scheduler works on a JVM without flight recorder.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.metrics;

import org.jspecify.annotations.NullMarked;
