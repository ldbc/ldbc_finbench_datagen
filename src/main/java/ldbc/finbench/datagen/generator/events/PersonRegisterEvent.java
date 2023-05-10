package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    // TODO: person can own multiple accounts
    public List<PersonOwnAccount> personRegister(List<Person> persons, AccountGenerator accountGenerator, int blockId,
                                                 GeneratorConfiguration conf) {
        resetState(blockId);
        List<PersonOwnAccount> personOwnAccounts = new ArrayList<>();

        for (Person p : persons) {
            Account account = accountGenerator.generateAccount();
            PersonOwnAccount personOwnAccount = PersonOwnAccount.createPersonOwnAccount(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                p, account);
            personOwnAccounts.add(personOwnAccount);
        }

        return personOwnAccounts;
    }
}
