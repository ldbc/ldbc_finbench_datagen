package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.SignIn;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SignInEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public SignInEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public void signIn(List<Medium> media, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < media.size(); i++) {
            Medium m = media.get(i);
            AccountGenerator accountGenerator = new AccountGenerator();

            if (sign()) {
                SignIn.createSignIn(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        m,
                        accountGenerator.generateAccount());
            }
        }
    }

    private boolean sign() {
        //TODO determine whether to generate signIn
        return true;
    }
}
