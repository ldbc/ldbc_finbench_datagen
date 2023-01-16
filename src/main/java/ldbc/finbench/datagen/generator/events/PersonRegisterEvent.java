package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonRegisterEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public void personRegister(List<Person> persons, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            AccountGenerator accountGenerator = new AccountGenerator();

            if (own()) {
                PersonOwnAccount.createPersonOwnAccount(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        accountGenerator.generateAccount());
            }
        }
    }

    private boolean own() {
        //TODO determine whether to generate own
        return true;
    }
}
