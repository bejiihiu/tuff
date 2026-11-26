/**
 * Extension points for an outer thread manager that wants to own compute threads and sizing.
 *
 * <p>The compute engine never imports such a manager. It asks these two ports instead, and the
 * defaults reproduce exactly what the engine did before the ports existed: same thread names, same
 * daemon flags, same processor budget. A server that has its own thread manager implements the ports
 * in its own module and hands them to
 * {@link kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers}.
 *
 * <p>Rules that keep the "no manager = no change" promise testable:
 * <ul>
 *     <li>every thread the engine and its pool create comes from {@link
 *     kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort};</li>
 *     <li>the processor budget comes either from an explicit number or from {@link
 *     kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort}, never from both at once.</li>
 * </ul>
 *
 * <p>Nothing here is supported plugin API. The types are {@code public} only because a manager in
 * another module has to implement them.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.spi;

import org.jspecify.annotations.NullMarked;
