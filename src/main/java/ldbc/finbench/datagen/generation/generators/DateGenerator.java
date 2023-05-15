package ldbc.finbench.datagen.generation.generators;

import java.time.LocalDateTime;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.PowerLawActivityDeleteDistribution;
import ldbc.finbench.datagen.generation.distribution.TimeDistribution;
import ldbc.finbench.datagen.util.DateTimeUtils;

// All the datetimes are generated at integral date except the ones of the Account2Account.
// The datetimes of the Account2Account are generated at the tweaked hour, random minutes and seconds.
public class DateGenerator {
    public static final long ONE_SECOND = 1000L;
    public static final long ONE_MINUTE = 60L * ONE_SECOND;
    public static final long ONE_HOUR = 60L * ONE_MINUTE;
    public static final long ONE_DAY = 24L * ONE_HOUR;
    public static final long SEVEN_DAYS = 7L * ONE_DAY;
    public static final long THIRTY_DAYS = 30L * ONE_DAY;
    public static final long ONE_YEAR = 365L * ONE_DAY;
    public static final long TWO_YEARS = 2L * ONE_YEAR;
    public static final long TEN_YEARS = 10L * ONE_YEAR;

    private final long simulationStart;
    private final long simulationEnd;
    private final PowerLawActivityDeleteDistribution powerLawActivityDeleteDistribution;
    private final TimeDistribution timeDistribution;

    public DateGenerator(LocalDateTime simulationStartYear, LocalDateTime simulationEndYear) {
        simulationStart = DateTimeUtils.toEpochMilli(simulationStartYear);
        simulationEnd = DateTimeUtils.toEpochMilli(simulationEndYear);
        powerLawActivityDeleteDistribution =
            new PowerLawActivityDeleteDistribution(DatagenParams.powerLawActivityDeleteFile);
        powerLawActivityDeleteDistribution.initialize();
        timeDistribution = new TimeDistribution(DatagenParams.hourDistributionFile);
    }

    public long randomDate(Random random, long minDate, long maxDate) {
        assert (minDate < maxDate) :
            "Invalid interval bounds. maxDate (" + maxDate + ") should be larger than minDate(" + minDate + ")";
        return (long) (random.nextDouble() * (maxDate - minDate) + minDate);
    }

    public Long randomPersonCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public Long randomCompanyCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public Long randomAccountCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public Long randomAccountDeletionDate(Random random, long creationDate, long maxDeletionDate) {
        return randomDate(random, creationDate + DatagenParams.deleteDelta, maxDeletionDate);
    }

    public Long randomMediumCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public long randomPersonToAccountDate(Random random, Person person, Account account) {
        long fromDate = Math.max(person.getCreationDate(), account.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToAccountDate(Random random, Company company, Account account) {
        long fromDate = Math.max(company.getCreationDate(), account.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToCompanyDate(Random random, Person person, Company company) {
        long fromDate = Math.max(person.getCreationDate(), company.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToCompanyDate(Random random, Company fromCompany, Company toCompany) {
        long fromDate =
            Math.max(fromCompany.getCreationDate(), toCompany.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomMediumToAccountDate(Random random, Medium medium, Account account) {
        long fromDate = Math.max(medium.getCreationDate(), account.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToPersonDate(Random random, Person fromPerson, Person toPerson) {
        long fromDate =
            Math.max(fromPerson.getCreationDate(), toPerson.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToLoanDate(Random random, Person person) {
        long fromDate = person.getCreationDate() + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToLoanDate(Random random, Company company) {
        long fromDate = company.getCreationDate() + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    // Only the hour distribution is tweaked in accordance with real profiling results.
    // The minute and second is generated randomly.
    public long randomAccountToAccountDate(Random random, Account from, Account to, long deletionDate) {
        long fromDate = Math.max(from.getCreationDate(), to.getCreationDate()) + DatagenParams.activityDelta;

        return randomDate(random, fromDate, Math.min(deletionDate, simulationEnd));
        // TODO: the frequent hour distribution is not applied here, which may cause deletion before creation.
        //       To support this, need to generate the time only on days.
        // long randHour = timeDistribution.nextHour(random);
        // long randMinute = timeDistribution.nextMinute(random);
        // long randSecond = timeDistribution.nextSecond(random);
        // return randDate + randHour * ONE_HOUR + randMinute * ONE_MINUTE + randSecond * ONE_SECOND;
    }

    public long randomLoanToAccountDate(Random random, Loan loan, Account account) {
        long fromDate = Math.max(loan.getCreationDate(), account.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomAccountToLoanDate(Random random, Account account, Loan loan) {
        long fromDate = Math.max(account.getCreationDate(), loan.getCreationDate()) + DatagenParams.activityDelta;
        return randomDate(random, fromDate, simulationEnd);
    }

    // Not used
    // TODO: if generated value outside the valid bound just pick the midpoint, this can be handled better.
    public long powerLawDeleteDate(Random random, long minDate, long maxDate) {
        long deletionDate =
            (long) (minDate + powerLawActivityDeleteDistribution.nextDouble(random.nextDouble(), random));
        if (deletionDate > maxDate) {
            deletionDate = minDate + (maxDate - minDate) / 2;
        }
        return deletionDate;
    }

    public long getSimulationStart() {
        return simulationStart;
    }

    public long getSimulationEnd() {
        return simulationEnd;
    }

    public Long getNetworkCollapse() {
        return getSimulationStart() + TEN_YEARS;
    }
}
