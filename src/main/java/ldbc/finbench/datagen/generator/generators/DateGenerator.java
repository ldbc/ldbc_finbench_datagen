package ldbc.finbench.datagen.generator.generators;

public class DateGenerator {
    public static final long ONE_DAY = 24L * 60L * 60L * 1000L;
    public static final long SEVEN_DAYS = 7L * ONE_DAY;
    private static final long THIRTY_DAYS = 30L * ONE_DAY;
    private static final long ONE_YEAR = 365L * ONE_DAY;
    private static final long TWO_YEARS = 2L * ONE_YEAR;
    private static final long TEN_YEARS = 10L * ONE_YEAR;

    private long simulationStart;
    private long simulationEnd;

    // TODO generate other data value

    public long getSimulationStart() {
        return simulationStart;
    }

    public long getSimulationEnd() {
        return simulationEnd;
    }
}
