package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

    public List<PersonOwnAccount> personRegister(List<Person> persons, AccountGenerator accountGenerator, int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);
        List<PersonOwnAccount> personOwnAccounts = new ArrayList<>();
        for (Person person : persons) {
            int numAccounts = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_PERSON)
                                        .nextInt(DatagenParams.maxAccountsPerOwner);
            // Each person has at least one account
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account account = accountGenerator.generateAccount(person.getCreationDate(), "person", blockId);
                PersonOwnAccount personOwnAccount =
                    PersonOwnAccount.createPersonOwnAccount(
                        randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_OWN_ACCOUNT_DATE), person,
                        account);
                personOwnAccounts.add(personOwnAccount);
            }
        }

        return personOwnAccounts;
    }
}
