package ldbc.finbench.datagen.generator.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonRegisterEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public List<PersonOwnAccount> personRegister(List<Person> persons, int blockId, GeneratorConfiguration conf) {
        random.setSeed(blockId);

        List<PersonOwnAccount> personOwnAccounts = new ArrayList<>();

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            AccountGenerator accountGenerator = new AccountGenerator(conf);

            if (own()) {
                PersonOwnAccount personOwnAccount = PersonOwnAccount.createPersonOwnAccount(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        accountGenerator.generateAccount());
                personOwnAccounts.add(personOwnAccount);
            }
        }

        return personOwnAccounts;
    }

    private boolean own() {
        //TODO determine whether to generate own
        return true;
    }
}
