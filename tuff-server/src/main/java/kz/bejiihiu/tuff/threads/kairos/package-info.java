/**
 * Kairos: Folia's deadline queue with a fund of tick threads that can grow and shrink.
 *
 * <p>The queue, comparator and runner state machine are ported from leafpile one to one; the fund is
 * the part this package adds. A single supervisor owns spawn and retire, the budget and supervisor
 * sources are seams the thread layer fills in, and a fixed fund keeps the first prototype's exact
 * behaviour. Nothing here touches ownership, region tasks or tick order.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.kairos;

import org.jspecify.annotations.NullMarked;
