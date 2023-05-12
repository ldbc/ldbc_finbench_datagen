package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.Transfer;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.DegreeDistribution;
import ldbc.finbench.datagen.generator.distribution.PowerLawFormulaDistribution;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class TransferEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random shuffleRandom;
    private final DegreeDistribution multiplicityDistribution;

    public TransferEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        shuffleRandom = new Random();
        multiplicityDistribution = DatagenParams.getMultiplicityDistribution();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        shuffleRandom.setSeed(seed);
        multiplicityDistribution.reset(seed);
    }

    // OutDegrees is shuffled with InDegrees
    private List<Long> shuffleToOutDegree(List<Long> inDegrees) {
        List<Long> outDegrees = new ArrayList<>(inDegrees);
        Collections.shuffle(outDegrees, shuffleRandom);
        return outDegrees;
    }

    public List<Transfer> transfer(List<Account> accounts, int blockId) {
        resetState(blockId);
        List<Transfer> transfers = new ArrayList<>();

        List<Long> inDegrees = new ArrayList<>();
        accounts.forEach(a -> inDegrees.add(a.getMaxInDegree()));
        List<Long> outDegrees = shuffleToOutDegree(inDegrees);

        long multiplicity =  multiplicityDistribution.nextDegree();


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
