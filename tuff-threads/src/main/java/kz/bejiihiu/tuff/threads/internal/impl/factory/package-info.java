/**
 * The only place that constructs threads and registers them for accounting.
 *
 * <p>Two styles live here. The provider may build an ordinary platform thread from a spec, which is
 * what new pools and existing executors use. It may also wrap a body so that a thread somebody else
 * constructed, with its own class, group and handler, is accounted while the body runs. The second
 * style exists for Folia region tick threads, whose concrete class is checked with {@code instanceof}
 * in the hot path and must never be replaced.
 *
 * <p>Virtual threads are not constructed here. They are a separate pool kind and are only ever
 * accounted through a wrapped body.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.factory;

import org.jspecify.annotations.NullMarked;
