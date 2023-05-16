package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
    private final List<Consumer<Loan>> subeventConsumers;
    private final List<Account> targetAccounts;
    private final List<Deposit> deposits;
    private final List<Repay> repays;

    private final List<Transfer> transfers;

    public LoanSubEvents(List<Account> targets) {
        randomFarm = new RandomGeneratorFarm();
        indexRandom = new Random(DatagenParams.defaultSeed);
        actionRandom = new Random(DatagenParams.defaultSeed);
        amountRandom = new Random(DatagenParams.defaultSeed);
        targetAccounts = targets;
        deposits = new ArrayList<>();
        repays = new ArrayList<>();
        transfers = new ArrayList<>();
        subeventConsumers = Arrays.asList(this::depositSubEvent, this::repaySubEvent, this::transferSubEvent);
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
                int actionIndex = actionRandom.nextInt(subeventConsumers.size());
                subeventConsumers.get(actionIndex).accept(loan);
            }
        }
    }

    private void depositSubEvent(Loan loan) {
        if (loan.getBalance() == 0) {
            return;
        }
        Account account = getAccount(loan);
        double amount = amountRandom.nextDouble() * loan.getBalance();
        Deposit deposit = Deposit.createDeposit(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), loan, account, amount);
        deposits.add(deposit);
    }

    private void repaySubEvent(Loan loan) {
        if (loan.getLoanAmount() == loan.getBalance()) {
            return;
        }

        Account account = getAccount(loan);
        double amount = amountRandom.nextDouble() * (loan.getLoanAmount() - loan.getBalance());
        Repay repay = Repay.createRepay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), account, loan, amount);
        repays.add(repay);
    }

    private void transferSubEvent(Loan loan) {
        Account account = getAccount(loan);
        Account target = targetAccounts.get(indexRandom.nextInt(targetAccounts.size()));
        double transferAmount = amountRandom.nextDouble() * DatagenParams.transferMaxAmount;
        Transfer transfer = actionRandom.nextDouble() < 0.5
            ? Transfer.createTransfer(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), account, target, transferAmount)
            : Transfer.createTransfer(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), target, account, transferAmount);
        transfers.add(transfer);
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
