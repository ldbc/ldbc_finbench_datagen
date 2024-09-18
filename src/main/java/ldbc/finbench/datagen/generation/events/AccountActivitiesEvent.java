package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.edges.Withdraw;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class AccountActivitiesEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final DegreeDistribution multiplicityDist;
    private final Random randIndex;
    private final Map<String, AtomicLong> multiplicityMap;
    private final float skippedRatio = 0.5f;
    private int maxSkippedCount = 10;

    public AccountActivitiesEvent() {
        randomFarm = new RandomGeneratorFarm();
        multiplicityDist = DatagenParams.getTransferMultiplicityDistribution();
        multiplicityDist.initialize();
        randIndex = new Random(DatagenParams.defaultSeed);
        multiplicityMap = new ConcurrentHashMap<>();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        multiplicityDist.reset(seed);
        randIndex.setSeed(seed);
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
    public List<Account> accountActivities(List<Account> accounts, List<Account> cards, int blockId) {
        resetState(blockId);
        Random pickAccountForWithdrawal = randomFarm.get(RandomGeneratorFarm.Aspect.ACCOUNT_WHETHER_WITHDRAW);

        LinkedList<Integer> availableToAccountIds = getIndexList(accounts.size());
        maxSkippedCount = Math.min(maxSkippedCount, (int) (skippedRatio * accounts.size()));

        // Simplified version of transfer process
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
        //                    Transfer.createTransfer(randomFarm, from, to, mindex);
        //                }
        //            }
        //        }
        for (int fromIndex = 0; fromIndex < accounts.size(); fromIndex++) {
            Account from = accounts.get(fromIndex);
            // TRANSFER: account transfer to other accounts
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
                if (skippedCount >= Math.min(maxSkippedCount, availableToAccountIds.size())) {
                    // System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break;
                }
            }
            // WITHDRAW: account withdraw to cards
            if (pickAccountForWithdrawal.nextDouble() < DatagenParams.accountWithdrawFraction) {
                for (int count = 0; count < DatagenParams.maxWithdrawals; count++) {
                    Account to = cards.get(randIndex.nextInt(cards.size()));
                    if (!cannotWithdraw(from, to)) {
                        Withdraw.createWithdraw(randomFarm, from, to, getMultiplicityIdAndInc(from, to));
                    }

                }
            }

        }
        return accounts;
    }

    // Transfer to self is not allowed
    private boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }

    private boolean cannotWithdraw(Account from, Account to) {
        return from.getType().equals("debit card")
            || from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to);
    }

    private long getMultiplicityIdAndInc(Account from, Account to) {
        String key = from.getAccountId() + "-" + to.getAccountId();
        AtomicLong atomicInt = multiplicityMap.computeIfAbsent(key, k -> new AtomicLong());
        return atomicInt.getAndIncrement();
    }
}
