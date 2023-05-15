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
    private final DegreeDistribution multiplicityDistribution;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        shuffleRandom = new Random(DatagenParams.defaultSeed);
        multiplicityDistribution = DatagenParams.getTsfMultiplicityDistribution();
        multiplicityDistribution.initialize();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        shuffleRandom.setSeed(seed);
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
        double prob = Math.pow(DatagenParams.baseProbCorrelated, Math.abs(distance));
        return ((randProb < prob) || (randProb < DatagenParams.limitProCorrelated));
    }

    // TODO: can not coalesce when large scale data generated in cluster
    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);
        // TODO: move shuffle to the main simulation process
        setOutDegreeWithShuffle(accounts);

        List<Transfer> allTransfers = new ArrayList<>();
        Random dateRandom = randomFarm.get(RandomGeneratorFarm.Aspect.DATE);

        for (int i = 0; i < accounts.size(); i++) {
            Account from = accounts.get(i);
            while (from.getAvaialbleOutDegree() != 0) {
                // i!=j: Transfer to self is not allowed
                for (int j = 0; j < accounts.size(); j++) {
                    Account to = accounts.get(j);
                    if (i == j || cannotTransfer(from, to)) {
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDistribution.nextDegree(), from.getAvaialbleOutDegree());
                    if (numTransfers <= to.getAvaialbleInDegree() && distanceProbOK(j - i)) {
                        for (int mindex = 0; mindex < numTransfers; mindex++) {
                            // Note: nearly impossible to generate same date
                            Transfer transfer = Transfer.createTransfer(dateRandom, from, to, mindex);
                            allTransfers.add(transfer);
                        }
                    }
                }
            }
        }
        return allTransfers;
    }

    public boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }
}
