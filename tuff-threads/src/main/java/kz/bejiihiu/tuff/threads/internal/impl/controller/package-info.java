/**
 * Admission, leases and the shared permit ledger.
 *
 * <p>The controller is the only place that hands out capacity. A pool owner asks for one permit before
 * a unit of work starts and releases it when the work ends; nothing here interrupts running work,
 * reschedules a task or touches a pool's queue.
 *
 * <p>Two rules keep it deadlock free by construction. First, permits are never nested: a request needs
 * one permit from exactly one pool, so no thread ever holds one permit while waiting for another.
 * Second, a region execution thread is rejected when it asks for a blocking wait, because that thread
 * is itself the resource the wait would depend on.
 *
 * <p>Critical pools - region ticks and network event loops - have two privileges: their own permits are
 * never shared, and they are exempt from the global background admission cap. The cap exists to keep
 * background work from starving the server, so letting it starve the server would defeat it.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.controller;

import org.jspecify.annotations.NullMarked;
