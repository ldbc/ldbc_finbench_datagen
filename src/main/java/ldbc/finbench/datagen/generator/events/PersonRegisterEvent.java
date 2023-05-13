package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountOwnerEnum;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.AccountGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonRegisterEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long is for personRegister, second for companyRegister

    public PersonRegisterEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    private void resetState(AccountGenerator accountGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        accountGenerator.resetState(random.nextLong());
    }

    // TODO: person can own multiple accounts
    public List<PersonOwnAccount> personRegister(List<Person> persons, AccountGenerator accountGenerator, int blockId,
                                                 GeneratorConfiguration conf) {
        resetState(accountGenerator, blockId);
        List<PersonOwnAccount> personOwnAccounts = new ArrayList<>();

        for (Person p : persons) {
            Account account = accountGenerator.generateAccount();
            account.setAccountOwnerEnum(AccountOwnerEnum.PERSON);
            account.setPersonOwner(p);
            PersonOwnAccount personOwnAccount = PersonOwnAccount.createPersonOwnAccount(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                p, account);
            personOwnAccounts.add(personOwnAccount);
        }

        return personOwnAccounts;
    }
}
