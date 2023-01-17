package ldbc.finbench.datagen.generator.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SubEvents {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public SubEvents() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }


    public List<Deposit> subEventDeposit(List<Loan> loans, List<Account> accounts,
                                         List<Account> depositedAccounts, int blockId) {
        random.setSeed(blockId);
        List<Deposit> deposits = new ArrayList<>();

        for (int i = 0; i < loans.size(); i++) {
            Loan l = loans.get(i);

            int accountIndex = randIndex.nextInt(accounts.size());
            Account account = accounts.get(accountIndex);
            depositedAccounts.add(account);

            if (deposit()) {
                Deposit deposit = Deposit.createDeposit(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        l,
                        account);
                deposits.add(deposit);
            }
        }
        return deposits;
    }

    public List<Transfer> subEventTransfer(List<Account> depositedAccounts,
                                           List<Account> transferredAccounts, int blockId) {
        random.setSeed(blockId);
        List<Transfer> transfers = new ArrayList<>();

        for (int i = 0; i < depositedAccounts.size(); i++) {
            Account a = depositedAccounts.get(i);

            int accountIndex = randIndex.nextInt(depositedAccounts.size());
            Account account = depositedAccounts.get(accountIndex);
            transferredAccounts.add(account);

            if (transfer()) {
                Transfer transfer = Transfer.createTransfer(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        a,
                        account);
                transfers.add(transfer);
            }
        }
        return transfers;
    }

    public List<Repay> subEventRepay(List<Account> transferredAccounts, List<Loan> loans, int blockId) {
        random.setSeed(blockId);
        List<Repay> repays = new ArrayList<>();

        for (int i = 0; i < transferredAccounts.size(); i++) {
            Account a = transferredAccounts.get(i);
            int loanIndex = randIndex.nextInt(loans.size());

            if (repay()) {
                Repay repay = Repay.createRepay(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        a,
                        loans.get(loanIndex));
                repays.add(repay);
            }
        }
        return repays;
    }

    private boolean deposit() {
        //TODO determine whether to generate deposit
        return true;
    }

    private boolean transfer() {
        //TODO determine whether to generate transfer
        return true;
    }

    private boolean repay() {
        //TODO determine whether to generate repay
        return true;
    }
}
