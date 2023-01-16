package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Withdraw;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WithdrawEvent {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public WithdrawEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public void withdraw(List<Account> accounts, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (with()) {
                Withdraw.createWithdraw(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        a,
                        accounts.get(accountIndex));
            }
        }
    }

    private boolean with() {
        //TODO determine whether to generate withdraw
        return true;
    }
}
