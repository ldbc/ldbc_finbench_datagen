package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.generators.LoanGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
    }

    private void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
    }

    public List<PersonApplyLoan> personLoan(List<Person> persons, LoanGenerator loanGenerator, int blockId,
                                            GeneratorConfiguration conf) {
        resetState(blockId);
        List<PersonApplyLoan> personApplyLoans = new ArrayList<>();

        for (Person p : persons) {
            PersonApplyLoan personApplyLoan = PersonApplyLoan.createPersonApplyLoan(
                randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                p,
                loanGenerator.generateLoan());
            personApplyLoans.add(personApplyLoan);
        }
        return personApplyLoans;
    }
}
