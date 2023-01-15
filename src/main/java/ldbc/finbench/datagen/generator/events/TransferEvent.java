package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public void transfer(List<Account> accounts, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (trans()) {
                Transfer.createTransfer(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        a,
                        accounts.get(accountIndex));
            }
        }
    }

    private boolean trans() {
        //TODO determine whether to generate transfer
        return true;
    }
}
