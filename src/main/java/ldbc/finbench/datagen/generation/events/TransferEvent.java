package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.DegreeDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random shuffleRandom;
    private final Random amountRandom;
    private final DegreeDistribution multiplicityDist;
    private double partRatio;

    public TransferEvent(double partRatio) {
        this.partRatio = partRatio;
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        shuffleRandom = new Random(DatagenParams.defaultSeed);
        amountRandom = new Random(DatagenParams.defaultSeed);
        multiplicityDist = DatagenParams.getTsfMultiplicityDistribution();
        multiplicityDist.initialize();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        shuffleRandom.setSeed(seed);
        amountRandom.setSeed(seed);
        multiplicityDist.reset(seed);
    }

    // Deprecated: OutDegrees is shuffled with InDegrees. It has been moved to the scala layer
    private void setOutDegreeWithShuffle(List<Account> accounts) {
        List<Long> degrees = accounts.parallelStream().map(Account::getMaxInDegree).collect(Collectors.toList());
        Collections.shuffle(degrees, shuffleRandom);
        IntStream.range(0, accounts.size()).parallel().forEach(i -> accounts.get(i).setMaxOutDegree(degrees.get(i)));
    }

    private List<Integer> getIndexList(int size) {
        List<Integer> indexList = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            indexList.add(i);
        }
        return indexList;
    }

    // Generation to parts will mess up the average degree(make it bigger than expected) caused by ceiling operations.
    // Also, it will mess up the long tail range of powerlaw distribution of degrees caused by 1 rounded to 2.
    // See the plot drawn by check_transfer.py for more details.
    public List<Transfer> transferPart(List<Account> accounts, int blockId) {
        resetState(blockId);

        List<Integer> availableToAccountIds = getIndexList(accounts.size()); // available transferTo accountIds

        // scale to percentage
        accounts.forEach(
            account -> {
                account.setMaxOutDegree((long) Math.ceil(account.getRawMaxOutDegree() * partRatio));
                account.setMaxInDegree((long) Math.ceil(account.getRawMaxInDegree() * partRatio));
            }
        );

        List<Transfer> transfers = new LinkedList<>();

        for (int i = 0; i < accounts.size(); i++) {
            Account from = accounts.get(i);
            while (from.getAvailableOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < availableToAccountIds.size(); j++) {
                    int toIndex = availableToAccountIds.get(j);
                    Account to = accounts.get(toIndex);
                    if (toIndex == i || cannotTransfer(from, to) || !distanceProbOK(j - i)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        transfers.add(Transfer.createTransferAndReturn(randomFarm, from, to, mindex,
                                                amountRandom.nextDouble() * DatagenParams.tsfMaxAmount));
                    }
                    if (to.getAvailableInDegree() == 0) {
                        availableToAccountIds.remove(j);
                        j--;
                    }
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                if (skippedCount == availableToAccountIds.size()) {
                    System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break; // end loop if all accounts are skipped
                }
                // System.out.println("Loop for " + from.getAccountId() + ", skippedCount: " + skippedCount + ", "
                //                       + "availableToAccountIds " + availableToAccountIds.size());
            }
        }
        return transfers;
    }

    private boolean distanceProbOK(int distance) {
        if (DatagenParams.tsfGenerationMode.equals("loose")) {
            return true;
        }
        double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
        double prob = Math.pow(DatagenParams.tsfBaseProbCorrelated, Math.abs(distance));
        return ((randProb < prob) || (randProb < DatagenParams.tsfLimitProCorrelated));
    }

    // Transfer to self is not allowed
    private boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }
}
