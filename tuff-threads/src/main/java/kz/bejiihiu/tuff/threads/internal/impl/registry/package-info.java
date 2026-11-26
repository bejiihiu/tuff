/**
 * Live thread bookkeeping: which pool owns which thread, and the counters behind creation and
 * termination rates.
 *
 * <p>Everything here is fed by the thread factory and by wrapped bodies; no pool report and no JVM
 * wide scan is mixed into the registry. Registration is reference counted, a thread belongs to exactly
 * one pool, and an entry disappears when the outermost unit of work that registered it ends, so the
 * registry never keeps references to dead threads around.
 *
 * <p>The unregistered thread report is the one place that looks at the whole JVM. It is meant for a
 * command or a bootstrap check, never for the sampler.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.registry;

import org.jspecify.annotations.NullMarked;
