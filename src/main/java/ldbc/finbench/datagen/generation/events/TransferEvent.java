package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final DegreeDistribution multiplicityDist;
    private final float skippedRatio = 0.5f;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        multiplicityDist = DatagenParams.getTransferMultiplicityDistribution();
        multiplicityDist.initialize();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        multiplicityDist.reset(seed);
    }

    private LinkedList<Integer> getIndexList(int size) {
        LinkedList<Integer> indexList = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            indexList.add(i);
        }
        return indexList;
    }

    // Generation to parts will mess up the average degree(make it bigger than expected) caused by ceiling operations.
    // Also, it will mess up the long tail range of powerlaw distribution of degrees caused by 1 rounded to 2.
    // See the plot drawn by check_transfer.py for more details.
    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);

        List<Transfer> transfers = new LinkedList<>();
        LinkedList<Integer> availableToAccountIds = getIndexList(accounts.size()); // available transferTo accountIds

        for (int i = 0; i < accounts.size(); i++) {
            Account from = accounts.get(i);
            while (from.getAvailableOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < availableToAccountIds.size(); j++) {
                    int toIndex = availableToAccountIds.get(j);
                    Account to = accounts.get(toIndex);
                    if (toIndex == i || cannotTransfer(from, to)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        transfers.add(Transfer.createTransfer(randomFarm, from, to, mindex));
                    }
                    if (to.getAvailableInDegree() == 0) {
                        availableToAccountIds.remove(j);
                        j--;
                    }
                    // TODO:
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                // end loop if all accounts are skipped
                if (skippedCount >= availableToAccountIds.size() * skippedRatio) {
                    System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break;
                }
            }
        }
        return transfers;
    }

    // Transfer to self is not allowed
    private boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }
}
