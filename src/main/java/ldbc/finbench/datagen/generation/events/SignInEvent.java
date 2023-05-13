package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.SignIn;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SignInEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public SignInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<SignIn> signIn(List<Medium> media, List<Account> accounts, int blockId, GeneratorConfiguration conf) {
        resetState(blockId);
        List<SignIn> signIns = new ArrayList<>();

        for (Medium m : media) {
            int accountIndex = randIndex.nextInt(accounts.size());
            SignIn signIn = SignIn.createSignIn(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                m,
                accounts.get(accountIndex));
            signIns.add(signIn);
        }
        return signIns;
    }
}
