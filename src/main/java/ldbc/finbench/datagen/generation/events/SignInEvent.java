package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.SignIn;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SignInEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;
    private final Random multiplicityRandom;
    private final Random accountsToSignRandom;

    public SignInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
        multiplicityRandom = new Random(DatagenParams.defaultSeed);
        accountsToSignRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
        multiplicityRandom.setSeed(seed);
        accountsToSignRandom.setSeed(seed);
    }

    public List<SignIn> signIn(List<Medium> media, List<Account> accounts, int blockId) {
        resetState(blockId);
        List<SignIn> signIns = new ArrayList<>();
        for (Medium medium : media) {
            int numAccountsToSign = accountsToSignRandom.nextInt(DatagenParams.maxAccountToSignIn);
            int signedCount = 0;
            while (signedCount < numAccountsToSign) {
                Account accountToSign = accounts.get(randIndex.nextInt(accounts.size()));
                if (cannotSignIn(medium, accountToSign)) {
                    continue;
                }
                int numSignIn = multiplicityRandom.nextInt(DatagenParams.maxSignInPerPair);
                for (int mid = 0; mid < numSignIn; mid++) {
                    SignIn signIn = SignIn.createSignIn(mid, randomFarm, medium, accountToSign);
                    signIns.add(signIn);
                }
                signedCount++;
            }
        }
        return signIns;
    }

    public boolean cannotSignIn(Medium from, Account to) {
        return from.getCreationDate() + DatagenParams.activityDelta > to.getDeletionDate();
    }
}
