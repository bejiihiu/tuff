package kz.bejiihiu.tuff.compute.internal.spi;

/**
 * Default {@link ComputeBudgetPort}: the JVM processor count.
 *
 * <p>This is what {@code ComputeSchedulers.create(settings)} always used. The server-side 80% rule
 * that subtracts region, chunk and IO threads stays where it is (the server bootstrap); this port
 * only fixes the number the engine starts with.
 */
final class LegacyComputeBudgetPort implements ComputeBudgetPort {

    static final LegacyComputeBudgetPort INSTANCE = new LegacyComputeBudgetPort();

    private LegacyComputeBudgetPort() {
    }

    @Override
    public int processors() {
        return Runtime.getRuntime().availableProcessors();
    }

    @Override
    public String source() {
        return "legacy";
    }
}
