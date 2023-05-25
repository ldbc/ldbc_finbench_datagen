package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
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

    public List<PersonApplyLoan> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        loanGenerator.resetState(blockId);
        List<PersonApplyLoan> personApplyLoans = new ArrayList<>();

        for (Person person : persons) {
            int numLoans =
                randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_PERSON).nextInt(DatagenParams.maxLoans);
            for (int i = 0; i < Math.max(1, numLoans); i++) {
                long applyDate =
                    Dictionaries.dates.randomPersonToLoanDate(
                        randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_DATE), person);
                Loan loan = loanGenerator.generateLoan(applyDate, "person", blockId);
                PersonApplyLoan personApplyLoan = PersonApplyLoan.createPersonApplyLoan(applyDate, person, loan);
                personApplyLoans.add(personApplyLoan);
            }
        }
        return personApplyLoans;
    }
}
