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
    private final DegreeDistribution multiplicityDist;

    public TransferEvent() {
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

    // TODO: move shuffle to the main simulation process
    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        if (DatagenParams.tsfGenerationMode.equals("strict")) {
            // strict mode will fill all allocated edge degrees and use distance probability
            // It is much slower. TODO: optimize it
            return transferStrictMode(accounts, blockId);
        } else if (DatagenParams.tsfGenerationMode.equals("loose")) {
            return transferLooseMode(accounts, blockId);
        } else {
            throw new IllegalArgumentException("Invalid transfer generation mode: " + DatagenParams.tsfGenerationMode);
        }
    }

    private List<Transfer> transferLooseMode(List<Account> accounts, int blockId) {
        resetState(blockId);
        setOutDegreeWithShuffle(accounts);

        List<Transfer> allTransfers = new ArrayList<>();
        Random dateRandom = randomFarm.get(RandomGeneratorFarm.Aspect.TRANSFER_DATE);

        // initial available transfer to account ids
        List<Integer> availableToAccountIds = new ArrayList<>();
        for (int index = 0; index < accounts.size(); index++) {
            availableToAccountIds.add(index);
        }

        // Note: be careful that here may be a infinite loop with some special parameters
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
                        allTransfers.add(Transfer.createTransfer(dateRandom, from, to, mindex,
                                                                 amountRandom.nextDouble()
                                                                     * DatagenParams.tsfMaxAmount));
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
                System.out.println("Loop for " + from.getAccountId() + ", skippedCount: " + skippedCount + ", "
                                       + "availableToAccountIds " + availableToAccountIds.size());
            }
        }
        return allTransfers;
    }

    private List<Transfer> transferStrictMode(List<Account> accounts, int blockId) {
        resetState(blockId);
        setOutDegreeWithShuffle(accounts);

        List<Transfer> allTransfers = new ArrayList<>();
        Random dateRandom = randomFarm.get(RandomGeneratorFarm.Aspect.TRANSFER_DATE);
        for (int i = 0; i < accounts.size(); i++) {
            Account from = accounts.get(i);
            while (from.getAvailableOutDegree() != 0) {
                int skippedCount = 0;
                for (int j = 0; j < accounts.size() && i != j; j++) {
                    Account to = accounts.get(j);
                    if (cannotTransfer(from, to) || !distanceProbOK(j - i)) {
                        skippedCount++;
                        continue;
                    }
                    long numTransfers = Math.min(multiplicityDist.nextDegree(),
                                                 Math.min(from.getAvailableOutDegree(), to.getAvailableInDegree()));
                    for (int mindex = 0; mindex < numTransfers; mindex++) {
                        allTransfers.add(Transfer.createTransfer(dateRandom, from, to, mindex,
                                                                 amountRandom.nextDouble()
                                                                     * DatagenParams.tsfMaxAmount));
                    }
                    if (from.getAvailableOutDegree() == 0) {
                        break;
                    }
                }
                if (skippedCount == accounts.size()) {
                    System.out.println("[Transfer] All accounts skipped for " + from.getAccountId());
                    break; // end loop if all accounts are skipped
                }
                // System.out.println("Loop for " + from.getAccountId() +", skippedCount: "+ skippedCount);
            }
        }
        return allTransfers;
    }

    // Transfer to self is not allowed
    public boolean cannotTransfer(Account from, Account to) {
        return from.getDeletionDate() < to.getCreationDate() + DatagenParams.activityDelta
            || from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate()
            || from.equals(to) || from.getAvailableOutDegree() == 0 || to.getAvailableInDegree() == 0;
    }
}
