/**
 * Flight recorder event definitions of the compute scheduler.
 *
 * <p>Every event is a plain {@link jdk.jfr.Event} subclass with {@code @Name("tuff.compute.*")}, no
 * stack trace and primitive or string fields only. Keeping them in their own package means the
 * recorder can decide whether to touch them at all: when no recording enables an event, the recorder
 * never instantiates it.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.metrics.event;

import org.jspecify.annotations.NullMarked;
