package ldbc.finbench.datagen.generator.events;

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
    private RandomGeneratorFarm randomFarm;
    private Random randIndex;
    private Random random;

    public SignInEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random();
        random = new Random();
    }

    public List<SignIn> signIn(List<Medium> media, List<Account> accounts, int blockId, GeneratorConfiguration conf) {
        random.setSeed(blockId);
        List<SignIn> signIns = new ArrayList<>();

        for (int i = 0; i < media.size(); i++) {
            Medium m = media.get(i);
            int accountIndex = randIndex.nextInt(accounts.size());

            if (sign()) {
                SignIn signIn = SignIn.createSignIn(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        m,
                        accounts.get(accountIndex));
                signIns.add(signIn);
            }
        }
        return signIns;
    }

    private boolean sign() {
        //TODO determine whether to generate signIn
        return true;
    }
}
