package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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
    private final Random random; // first random long is for personRegister, second for companyRegister
    private final Random numAccountsRandom;

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random(DatagenParams.defaultSeed);
        numAccountsRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(AccountGenerator accountGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        long newSeed = random.nextLong();
        accountGenerator.resetState(newSeed);
        numAccountsRandom.setSeed(newSeed);
    }

    public List<PersonOwnAccount> personRegister(List<Person> persons, AccountGenerator accountGenerator, int blockId) {
        resetState(accountGenerator, blockId);
        List<PersonOwnAccount> personOwnAccounts = new ArrayList<>();
        for (Person person : persons) {
            // Each person has at least one account
            for (int i = 0; i < Math.max(1, numAccountsRandom.nextInt(DatagenParams.maxAccountsPerOwner)); i++) {
                Account account = accountGenerator.generateAccount();
                PersonOwnAccount personOwnAccount =
                    PersonOwnAccount.createPersonOwnAccount(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person,
                                                            account);
                personOwnAccounts.add(personOwnAccount);
            }
        }

        return personOwnAccounts;
    }
}
