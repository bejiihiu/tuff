/**
 * Flight recorder events of the unified thread manager.
 *
 * <p>Every event lives in the {@code tuff.threads} namespace and is a plain gauge or lifecycle record:
 * no stack trace, no allocation of strings before {@code isEnabled()} passed. A recording that enables
 * none of them costs one boolean check per hook, which is the only reason the manager can emit
 * unconditionally instead of guessing whether somebody is recording.
 *
 * <p>Names and field names are part of the recording schema. Renaming an event breaks saved
 * recordings, dashboards and {@code jfr print} queries, so they are treated as frozen once published.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.events;

import org.jspecify.annotations.NullMarked;
