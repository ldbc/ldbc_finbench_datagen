package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random random; // first random long is for personApply, second for companyApply

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    private void resetState(LoanGenerator loanGenerator, int seed) {
        randomFarm.resetRandomGenerators(seed);
        random.setSeed(7654321L + 1234567 * seed);
        loanGenerator.resetState(random.nextLong());
    }

    public List<PersonApplyLoan> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId) {
        resetState(loanGenerator, blockId);
        List<PersonApplyLoan> personApplyLoans = new ArrayList<>();

        for (Person person : persons) {
            long applyDate = Dictionaries.dates.randomPersonToLoanDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                                                                       person);
            PersonApplyLoan personApplyLoan = PersonApplyLoan.createPersonApplyLoan(
                applyDate, person, loanGenerator.generateLoan(applyDate));
            personApplyLoans.add(personApplyLoan);
        }
        return personApplyLoans;
    }
}