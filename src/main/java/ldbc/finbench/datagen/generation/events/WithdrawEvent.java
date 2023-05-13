package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Withdraw;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class WithdrawEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public WithdrawEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
    }


    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<Withdraw> withdraw(List<Account> accounts, int blockId) {
        resetState(blockId);
        List<Withdraw> withdraws = new ArrayList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());
            Withdraw withdraw = Withdraw.createWithdraw(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                a,
                accounts.get(accountIndex));
            withdraws.add(withdraw);
        }
        return withdraws;
    }
}
