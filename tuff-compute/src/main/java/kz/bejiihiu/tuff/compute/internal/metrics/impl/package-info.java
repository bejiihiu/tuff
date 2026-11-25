/**
 * Default implementations of the metrics and flight recorder contracts.
 *
 * <p>Contains the collector that the engine instantiates, the immutable snapshot records it returns,
 * the compact latency histogram behind the latencies and the flight recorder bridge with its event
 * classes. Nothing in here is part of the public scheduler API; the engine is the only expected
 * caller.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import org.jspecify.annotations.NullMarked;
