package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long is for personApply, second for companyApply
    private final Random numLoanRandom;

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random(DatagenParams.defaultSeed);
        numLoanRandom = new Random(DatagenParams.defaultSeed);
    }

    private void resetState(LoanGenerator loanGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        long newSeed = random.nextLong();
        loanGenerator.resetState(newSeed);
        numLoanRandom.setSeed(newSeed);
    }

    public List<PersonApplyLoan> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId) {
        resetState(loanGenerator, blockId);
        List<PersonApplyLoan> personApplyLoans = new ArrayList<>();

        for (Person person : persons) {
            for (int i = 0; i < numLoanRandom.nextInt(DatagenParams.maxLoans); i++) {
                long applyDate =
                    Dictionaries.dates.randomPersonToLoanDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person);
                Loan loan = loanGenerator.generateLoan(applyDate);
                PersonApplyLoan personApplyLoan = PersonApplyLoan.createPersonApplyLoan(applyDate, person, loan);
                personApplyLoans.add(personApplyLoan);
            }
        }
        return personApplyLoans;
    }
}
