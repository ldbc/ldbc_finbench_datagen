package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import ldbc.finbench.datagen.entities.edges.CompanyOwnAccount;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class LoanSubEvents implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random indexRandom;
    private final Random actionRandom;
    private final Random amountRandom;
    private final List<Consumer<Loan>> consumers;
    private final List<Account> targetAccounts;
    private final List<Deposit> deposits;
    private final List<Repay> repays;
    private final List<Transfer> transfers;
    // Note: Don't make it static. It will be accessed by different Spark workers, which makes multiplicity wrong.
    private final Map<String, AtomicLong> multiplicityMap;

    public LoanSubEvents(List<Account> targets) {
        multiplicityMap = new ConcurrentHashMap<>();
        randomFarm = new RandomGeneratorFarm();
        indexRandom = new Random(DatagenParams.defaultSeed);
        actionRandom = new Random(DatagenParams.defaultSeed);
        amountRandom = new Random(DatagenParams.defaultSeed);
        targetAccounts = targets;
        deposits = new ArrayList<>();
        repays = new ArrayList<>();
        transfers = new ArrayList<>();
        // Add all defined subevents to the consumers list
        consumers = Arrays.asList(this::depositSubEvent,
                                  this::repaySubEvent,
                                  this::transferSubEvent);
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        indexRandom.setSeed(seed);
        actionRandom.setSeed(seed);
        amountRandom.setSeed(seed);
    }

    public List<Deposit> getDeposits() {
        return deposits;
    }

    public List<Repay> getRepays() {
        return repays;
    }

    public List<Transfer> getTransfers() {
        return transfers;
    }

    public void afterLoanApplied(List<Loan> loans, int blockId) {
        resetState(blockId);
        for (Loan loan : loans) {
            int count = 0;
            while (count++ < DatagenParams.numLoanActions) {
                Consumer<Loan> consumer = consumers.get(actionRandom.nextInt(consumers.size()));
                consumer.accept(loan);
            }
        }
    }

    private void depositSubEvent(Loan loan) {
        Account account = getAccount(loan);
        if (loan.getBalance() == 0 || cannotDeposit(loan, account)) {
            return;
        }
        double amount = amountRandom.nextDouble() * loan.getBalance();
        Deposit deposit =
            Deposit.createDeposit(randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_SUBEVENTS_DATE), loan, account,
                                  amount);
        deposits.add(deposit);
    }

    private void repaySubEvent(Loan loan) {
        Account account = getAccount(loan);
        if (loan.getLoanAmount() == loan.getBalance() || cannotRepay(account, loan)) {
            return;
        }

        double amount = amountRandom.nextDouble() * (loan.getLoanAmount() - loan.getBalance());
        Repay repay =
            Repay.createRepay(randomFarm.get(RandomGeneratorFarm.Aspect.LOAN_SUBEVENTS_DATE), account, loan, amount);
        repays.add(repay);
    }

    public long getMultiplicityIdAndInc(Account from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }


    private void transferSubEvent(Loan loan) {
        Account account = getAccount(loan);
        Account target = targetAccounts.get(indexRandom.nextInt(targetAccounts.size()));
        if (actionRandom.nextDouble() < 0.5) {
            if (!cannotTransfer(account, target)) {
                transfers.add(
                    Transfer.createLoanTransferAndReturn(randomFarm, account, target,
                                                         getMultiplicityIdAndInc(account, target),
                                                         amountRandom.nextDouble() * DatagenParams.tsfMaxAmount));
            }
        } else {
            if (!cannotTransfer(target, account)) {
                transfers.add(
                    Transfer.createLoanTransferAndReturn(randomFarm, target, account,
                                                         getMultiplicityIdAndInc(target, account),
                                                         amountRandom.nextDouble() * DatagenParams.tsfMaxAmount));
            }
        }
    }

    public boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    public boolean cannotDeposit(Loan from, Account to) {
        return from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }

    public boolean cannotRepay(Account from, Loan to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta;
    }

    private Account getAccount(Loan loan) {
        if (loan.getOwnerType() == PersonOrCompany.PERSON) {
            List<PersonOwnAccount> poa = loan.getOwnerPerson().getPersonOwnAccounts();
            return poa.get(indexRandom.nextInt(poa.size())).getAccount();
        } else {
            List<CompanyOwnAccount> coa = loan.getOwnerCompany().getCompanyOwnAccounts();
            return coa.get(indexRandom.nextInt(coa.size())).getAccount();
        }
    }
}
