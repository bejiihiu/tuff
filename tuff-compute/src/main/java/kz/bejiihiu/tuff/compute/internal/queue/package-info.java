/**
 * Queue entries, the scheduling queue and per consumer quotas.
 *
 * <p>The queue owns all waiting tasks and decides the order in which runnable tasks are offered to
 * workers. It knows nothing about execution, futures or workers: it accepts entries, hands them out
 * and reports its own counters.
 *
 * <p>Three policies live in this package and must be implemented together, because they share the
 * same ordering mathematics: the bounded queue itself, the fairness between consumers (priority,
 * aging, weighted fair share) and admission control that stops a consumer from exceeding its
 * concurrency limit.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.queue;

import org.jspecify.annotations.NullMarked;
