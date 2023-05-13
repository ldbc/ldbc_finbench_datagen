package ldbc.finbench.datagen.generator.generators;

import java.time.LocalDateTime;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.PowerLawActivityDeleteDistribution;
import ldbc.finbench.datagen.generator.distribution.TimeDistribution;
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

    // TODO: use Degree information to determine when an account's deleted
    public Long randomAccountDeletionDate(Random random, long creationDate, long maxDeletionDate) {
        long accountCreationDate = creationDate + DatagenParams.delta;
        return randomDate(random, accountCreationDate, maxDeletionDate);
    }

    public Long randomMediumCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public Long randomLoanCreationDate(Random random) {
        return randomDate(random, simulationStart, simulationEnd);
    }

    public long randomPersonToAccountDate(Random random, Person person, Account account) {
        long fromDate = Math.max(person.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToAccountDate(Random random, Company company, Account account) {
        long fromDate = Math.max(company.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToCompanyDate(Random random, Person person, Company company) {
        long fromDate = Math.max(person.getCreationDate(), company.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToCompanyDate(Random random, Company fromCompany, Company toCompany) {
        long fromDate =
            Math.max(fromCompany.getCreationDate(), toCompany.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomMediumToAccountDate(Random random, Medium medium, Account account) {
        long fromDate = Math.max(medium.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToPersonDate(Random random, Person fromPerson, Person toPerson) {
        long fromDate = Math.max(fromPerson.getCreationDate(), toPerson.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomPersonToLoanDate(Random random, Person person, Loan loan) {
        long fromDate = Math.max(person.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomCompanyToLoanDate(Random random, Company company, Loan loan) {
        long fromDate = Math.max(company.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    // Only the hour distribution is tweaked in accordance with real profiling results.
    // The minute and second is generated randomly.
    public long randomAccountToAccountDate(Random random, Account fromAccount, Account toAccount) {
        long fromDate =
            Math.max(fromAccount.getCreationDate(), toAccount.getCreationDate()) + DatagenParams.delta;
        // TODO: the date here is not rounded
        long randDate = randomDate(random, fromDate, simulationEnd);
        long randHour = timeDistribution.nextHour(random);
        long randMinute = timeDistribution.nextMinute(random);
        long randSecond = timeDistribution.nextSecond(random);
        return randDate + randHour * ONE_HOUR + randMinute * ONE_MINUTE + randSecond * ONE_SECOND;
    }

    public long randomLoanToAccountDate(Random random, Loan loan, Account account) {
        long fromDate = Math.max(loan.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long randomAccountToLoanDate(Random random, Account account, Loan loan) {
        long fromDate = Math.max(account.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        return randomDate(random, fromDate, simulationEnd);
    }

    public long powerLawDeleteDate(Random random, long minDate, long maxDate) {
        long deletionDate =
            (long) (minDate + powerLawActivityDeleteDistribution.nextDouble(random.nextDouble(), random));
        // TODO: if generated value outside the valid bound just pick the midpoint, this can be handled better.
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
