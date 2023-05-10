package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        random.setSeed(blockId);
        List<Transfer> transfers = new ArrayList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account a = accounts.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (trans()) {
                Transfer transfer = Transfer.createTransfer(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    a,
                    accounts.get(accountIndex));
                transfers.add(transfer);
            }
        }
        return transfers;
    }

    private boolean trans() {
        //TODO determine whether to generate transfer
        return true;
    }
}
