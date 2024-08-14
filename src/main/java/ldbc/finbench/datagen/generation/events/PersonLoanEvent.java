package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Person> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        loanGenerator.resetState(blockId);

        Collections.shuffle(persons, randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_LOAN_SHUFFLE));
        int numPersonsToTake = (int)(persons.size() * DatagenParams.personLoanFraction);
        for (int i = 0; i < numPersonsToTake; i++) {
            Person from = persons.get(i);
            int numLoans =
                randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_PERSON).nextInt(DatagenParams.maxLoans);
            for (int j = 0; j < Math.max(1, numLoans); j++) {
                long applyDate =
                    Dictionaries.dates.randomPersonToLoanDate(
                        randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_DATE), from);
                String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
                    randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_ORGANIZATION));
                Loan to = loanGenerator.generateLoan(applyDate, "person", blockId);
                PersonApplyLoan.createPersonApplyLoan(applyDate, from, to, organization);
            }
        }

        return persons;
    }
}
