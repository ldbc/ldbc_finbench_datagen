package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.generators.AccountGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Person> personRegister(List<Person> persons, AccountGenerator accountGenerator, int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);

        Random numAccRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_PERSON);
        for (Person person : persons) {
            int numAccounts = numAccRand.nextInt(DatagenParams.maxAccountsPerOwner);
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account account = accountGenerator.generateAccount(person.getCreationDate(), "person", blockId);
                PersonOwnAccount.createPersonOwnAccount(randomFarm, person, account, account.getCreationDate());
            }
        }

        return persons;
    }
}
