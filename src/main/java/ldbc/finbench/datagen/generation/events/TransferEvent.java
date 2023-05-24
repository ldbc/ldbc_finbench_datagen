package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
    private final DegreeDistribution multiplicityDistribution;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        shuffleRandom = new Random(DatagenParams.defaultSeed);
        amountRandom = new Random(DatagenParams.defaultSeed);
        multiplicityDistribution = DatagenParams.getTsfMultiplicityDistribution();
        multiplicityDistribution.initialize();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        shuffleRandom.setSeed(seed);
        amountRandom.setSeed(seed);
        multiplicityDistribution.reset(seed);
    }

    // OutDegrees is shuffled with InDegrees
    private void setOutDegreeWithShuffle(List<Account> accounts) {
        List<Long> degrees = new ArrayList<>();
        accounts.forEach(a -> degrees.add(a.getMaxInDegree()));
        Collections.shuffle(degrees, shuffleRandom);
        for (int i = 0; i < accounts.size(); i++) {
            accounts.get(i).setMaxOutDegree(degrees.get(i));
        }
    }

    private boolean distanceProbOK(int distance) {
        double randProb = randomFarm.get(RandomGeneratorFarm.Aspect.UNIFORM).nextDouble();
        double prob = Math.pow(DatagenParams.tsfBaseProbCorrelated, Math.abs(distance));
        return ((randProb < prob) || (randProb < DatagenParams.tsfLimitProCorrelated));
    }

    // TODO: can not coalesce when large scale data generated in cluster
    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);
        // TODO: move shuffle to the main simulation process
        setOutDegreeWithShuffle(accounts);

        List<Transfer> allTransfers = new ArrayList<>();
        Random dateRandom = randomFarm.get(RandomGeneratorFarm.Aspect.DATE);

        // Note: be careful that here may be a infinite loop with some special parameters
        for (int i = 0; i < accounts.size(); i++) {
            Account from = accounts.get(i);
            // int loopCount = 0;
            while (from.getAvaialbleOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < accounts.size(); j++) {
                    Account to = accounts.get(j);
                    if (cannotTransfer(from, to) || !distanceProbOK(j - i)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDistribution.nextDegree(),
                                                 Math.min(from.getAvaialbleOutDegree(), to.getAvaialbleInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        allTransfers.add(Transfer.createTransfer(dateRandom, from, to, mindex,
                                                                 amountRandom.nextDouble()
                                                                     * DatagenParams.tsfMaxAmount));
                    }
                    if (from.getAvaialbleOutDegree() == 0) {
                        break;
                    }
                }
                if (skippedCount == accounts.size()) {
                    System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break; // end loop if all accounts are skipped
                }
                // System.out.println("Loop for " + from.getAccountId() + " " + loopCount++ +", skippedCount: "+
                // skippedCount);
            }
        }
        return allTransfers;
    }

    // Transfer to self is not allowed
    public boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvaialbleOutDegree() == 0 || to.getAvaialbleInDegree() == 0;
    }
}
