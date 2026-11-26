/**
 * Storage of pool descriptions and of the probes that read them.
 *
 * <p>The two registries are separate on purpose: anybody may describe a pool, but only the owner holds
 * the object that can observe it. Writes are rare and serialised; every read is lock free and returns
 * an immutable snapshot, so a command handler never blocks bootstrap and never sees a half applied
 * reload.
 *
 * <p>Nothing here creates, resizes or stops a thread. A spec is an identity card, a probe is a
 * thermometer. If a pool needs more, that is the factory, the controller or the runtime, not this
 * package.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.pools;

import org.jspecify.annotations.NullMarked;
