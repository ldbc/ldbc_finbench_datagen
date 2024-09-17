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
    private int maxSkippedCount = 10;

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
    public List<Account> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);

        LinkedList<Integer> availableToAccountIds = getIndexList(accounts.size());
        maxSkippedCount = Math.min(maxSkippedCount, (int) (skippedRatio * accounts.size()));

        //        for (int i = 0; i < accounts.size(); i++) {
        //            Account from = accounts.get(i);
        //            int skippedCount = 0;
        //            for (int j = i + 1; j < accounts.size(); j++) {
        //                // termination
        //                if (skippedCount >= maxSkippedCount || from.getAvailableOutDegree() == 0) {
        //                    break;
        //                }
        //                Account to = accounts.get(j);
        //                if (j == i || cannotTransfer(from, to)) {
        //                    skippedCount++;
        //                    continue;
        //                }
        //                long numTransfers = Math.min(multiplicityDist.nextDegree(),
        //                                             Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree
        //                                             ()));
        //                for (int mindex = 0; mindex < numTransfers; mindex++) {
        //                    transfers.add(Transfer.createTransfer(randomFarm, from, to, mindex));
        //                }
        //            }
        //        }
        for (int fromIndex = 0; fromIndex < accounts.size(); fromIndex++) {
            Account from = accounts.get(fromIndex);
            while (from.getAvailableOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < availableToAccountIds.size(); j++) {
                    int toIndex = availableToAccountIds.get(j);
                    Account to = accounts.get(toIndex);
                    if (toIndex == fromIndex || cannotTransfer(from, to)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        Transfer.createTransfer(randomFarm, from, to, mindex);
                    }
                    if (to.getAvailableInDegree() == 0) {
                        availableToAccountIds.remove(j);
                        j--;
                    }
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                // if (skippedCount == availableToAccountIds.size()) {
                if (skippedCount >= maxSkippedCount) {
                    // System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break;
                }
            }
        }
        return accounts;
    }

    private boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }
}
