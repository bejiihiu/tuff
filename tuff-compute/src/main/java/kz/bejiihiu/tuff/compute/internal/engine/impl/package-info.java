/**
 * Default implementation of the compute engine and its public scheduler facade.
 *
 * <p>The package contains the whole lifecycle in one place: submission validation, the real
 * {@link java.util.concurrent.CompletableFuture} based task handle, the dispatcher that binds the
 * queue to consumer quotas, the worker loop, the queue janitor and the assembly factory the server
 * bootstrap uses. Nothing outside {@code kz.bejiihiu.tuff.compute.internal} should depend on these
 * classes except the single factory in {@link kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers}.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.engine.impl;

import org.jspecify.annotations.NullMarked;
