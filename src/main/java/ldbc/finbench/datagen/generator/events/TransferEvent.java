package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);
        List<Transfer> transfers = new ArrayList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());
            Transfer transfer = Transfer.createTransfer(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                a,
                accounts.get(accountIndex));
            transfers.add(transfer);
        }
        return transfers;
    }
}
