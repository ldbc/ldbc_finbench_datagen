package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
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
    private final double probLoan;

    public PersonLoanEvent(double probLoan) {
        this.probLoan = probLoan;
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<Person> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId) {
        resetState(blockId);
        loanGenerator.resetState(blockId);

        persons.forEach(person -> {
            if (randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_WHETHER_LOAN).nextDouble() < probLoan) {
                int numLoans =
                    randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LOANS_PER_PERSON).nextInt(DatagenParams.maxLoans);
                for (int i = 0; i < Math.max(1, numLoans); i++) {
                    long applyDate =
                        Dictionaries.dates.randomPersonToLoanDate(
                            randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_DATE), person);
                    String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
                        randomFarm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_ORGANIZATION));
                    Loan loan = loanGenerator.generateLoan(applyDate, "person", blockId);
                    PersonApplyLoan.createPersonApplyLoan(applyDate, person, loan, organization);
                }
            }
        });

        return persons;
    }
}
