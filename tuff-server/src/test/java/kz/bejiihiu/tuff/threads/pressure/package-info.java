/**
 * Unit tests of the compute pool's tick pressure policy.
 *
 * <p>The controller is driven with a hand-written clock and a fake region signal, and the monitor with
 * a recording budget sink, so the tests state exactly which second and which reserve they mean. No
 * test starts a server and none sleeps on a real window.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.pressure;

import org.jspecify.annotations.NullMarked;
