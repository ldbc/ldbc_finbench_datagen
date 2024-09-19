package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.edges.PersonGuaranteePerson;
import ldbc.finbench.datagen.entities.edges.PersonOwnAccount;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.AccountGenerator;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonActivitiesEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public PersonActivitiesEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    // Generate accounts, guarantees, and loans for persons
    public List<Person> personActivities(List<Person> persons, AccountGenerator accountGenerator,
                                         LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        accountGenerator.resetState(blockId);

        Random numAccRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_PER_PERSON);

        Random pickPersonGuaRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_PERSON_GUARANTEE);
        Random numGuaranteesRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_GUARANTEES_PER_PERSON);

        Random pickPersonLoanRand = randomFarm.get(RandomGeneratorFarm.Aspect.PICK_PERSON_LOAN);
        Random numLoansRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_PERSON);
        Random dateRand = randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_DATE);

        for (Person from : persons) {
            // register accounts
            int numAccounts = numAccRand.nextInt(DatagenParams.maxAccountsPerOwner);
            for (int i = 0; i < Math.max(1, numAccounts); i++) {
                Account to = accountGenerator.generateAccount(from.getCreationDate(), "person", blockId);
                PersonOwnAccount.createPersonOwnAccount(randomFarm, from, to, to.getCreationDate());
            }
            // guarantee other persons
            if (pickPersonGuaRand.nextDouble() < DatagenParams.personGuaranteeFraction) {
                int numGuarantees = numGuaranteesRand.nextInt(DatagenParams.maxTargetsToGuarantee);
                for (int i = 0; i < Math.max(1, numGuarantees); i++) {
                    Person to = persons.get(randIndex.nextInt(persons.size()));
                    if (from.canGuarantee(to)) {
                        PersonGuaranteePerson.createPersonGuaranteePerson(randomFarm, from, to);
                    }
                }
            }
            // apply loans
            if (pickPersonLoanRand.nextDouble() < DatagenParams.personLoanFraction) {
                int numLoans = numLoansRand.nextInt(DatagenParams.maxLoans);
                for (int i = 0; i < Math.max(1, numLoans); i++) {
                    long applyDate = Dictionaries.dates.randomPersonToLoanDate(dateRand, from);
                    Loan to = loanGenerator.generateLoan(applyDate, "person", blockId);
                    PersonApplyLoan.createPersonApplyLoan(randomFarm, applyDate, from, to);
                }
            }
        }

        return persons;
    }
}
