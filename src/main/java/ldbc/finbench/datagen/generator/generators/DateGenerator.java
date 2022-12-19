package ldbc.finbench.datagen.generator.generators;

import java.time.LocalDate;
import java.util.Random;
import ldbc.finbench.datagen.generator.distribution.PowerLawDistribution;
import ldbc.finbench.datagen.util.DateUtils;

public class DateGenerator {
    public static final long ONE_DAY = 24L * 60L * 60L * 1000L;
    public static final long SEVEN_DAYS = 7L * ONE_DAY;
    private static final long THIRTY_DAYS = 30L * ONE_DAY;
    private static final long ONE_YEAR = 365L * ONE_DAY;
    private static final long TWO_YEARS = 2L * ONE_YEAR;
    private static final long TEN_YEARS = 10L * ONE_YEAR;

    private long simulationStart;
    private long simulationEnd;
    private PowerLawDistribution powerLawDistribution;

    public DateGenerator(LocalDate simulationStartYear, LocalDate simulationEndYear) {
        simulationStart = DateUtils.toEpochMilli(simulationStartYear);
        simulationEnd = DateUtils.toEpochMilli(simulationEndYear);
        powerLawDistribution = new PowerLawDistribution(0.5,0.5);
    }

    public Long randomPersonCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    public Long randomCompanyCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    public Long randomAccountCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    public Long randomMediumCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    public Long randomLoanCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    public long getSimulationStart() {
        return simulationStart;
    }

    public long getSimulationEnd() {
        return simulationEnd;
    }
}
