package ldbc.finbench.datagen.generator.generators;

import java.time.LocalDate;
import java.util.Random;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.DatagenParams;
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

    public long randomDate(Random random, long minDate, long maxDate) {
        assert (minDate < maxDate) : "Invalid interval bounds. maxDate ("
                + maxDate + ") should be larger than minDate(" + minDate + ")";
        return (long) (random.nextDouble() * (maxDate - minDate) + minDate);
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

    public long randomPersonToAccountDate(Random random, Person person, Account account) {
        long fromDate = Math.max(person.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomCompanyToAccountDate(Random random, Company company, Account account) {
        long fromDate = Math.max(company.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomPersonToCompanyDate(Random random, Person person, Company company) {
        long fromDate = Math.max(person.getCreationDate(), company.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomCompanyToCompanyDate(Random random, Company fromCompany, Company toCompany) {
        long fromDate = Math.max(fromCompany.getCreationDate(), toCompany.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomMediumToAccountDate(Random random, Medium medium, Account account) {
        long fromDate = Math.max(medium.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomPersonToPersonDate(Random random, Person fromPerson, Person toPerson) {
        long fromDate = Math.max(fromPerson.getCreationDate(), toPerson.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomPersonToLoanDate(Random random, Person person, Loan loan) {
        long fromDate = Math.max(person.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomCompanyToLoanDate(Random random, Company company, Loan loan) {
        long fromDate = Math.max(company.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomAccountToAccountDate(Random random, Account fromAccount, Account toAccount) {
        long fromDate = Math.max(fromAccount.getCreationDate(), toAccount.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomLoanToAccountDate(Random random, Loan loan, Account account) {
        long fromDate = Math.max(loan.getCreationDate(), account.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long randomAccountToLoanDate(Random random, Account account, Loan loan) {
        long fromDate = Math.max(account.getCreationDate(), loan.getCreationDate()) + DatagenParams.delta;
        long toDate = simulationEnd;
        return randomDate(random, fromDate, toDate);
    }

    public long getSimulationStart() {
        return simulationStart;
    }

    public long getSimulationEnd() {
        return simulationEnd;
    }
}
