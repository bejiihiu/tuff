/**
 * Scheduler-independent observation of executed region tick bodies.
 *
 * <p>The region handle calls one observer after every body it runs, whatever scheduler produced the
 * body. With nothing installed the tick path pays one volatile read and a {@code null} check; with an
 * observer installed the same seam writes the per-tick JFR event that the measurement harness reads.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.observe;

import org.jspecify.annotations.NullMarked;
