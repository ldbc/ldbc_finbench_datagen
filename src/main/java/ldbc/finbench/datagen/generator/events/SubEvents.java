package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Deposit;
import ldbc.finbench.datagen.entities.edges.Repay;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SubEvents implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public SubEvents() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }


    public List<Deposit> subEventDeposit(List<Loan> loans, List<Account> accounts, int blockId) {
        random.setSeed(blockId);
        List<Deposit> deposits = new ArrayList<>();

        for (int i = 0; i < loans.size(); i++) {
            Loan l = loans.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (deposit()) {
                Deposit deposit = Deposit.createDeposit(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    l,
                    accounts.get(accountIndex));
                deposits.add(deposit);
            }
        }
        return deposits;
    }

    public List<Transfer> subEventTransfer(List<Account> accounts, int blockId) {
        random.setSeed(blockId);
        List<Transfer> transfers = new ArrayList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (transfer()) {
                Transfer transfer = Transfer.createTransfer(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    a,
                    accounts.get(accountIndex));
                transfers.add(transfer);
            }
        }
        return transfers;
    }

    public List<Repay> subEventRepay(List<Account> accounts, List<Loan> loans, int blockId) {
        random.setSeed(blockId);
        List<Repay> repays = new ArrayList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
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
