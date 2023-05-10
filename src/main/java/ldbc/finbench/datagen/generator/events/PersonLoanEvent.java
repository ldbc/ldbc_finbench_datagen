package ldbc.finbench.datagen.generator.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.LoanGenerator;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent implements Serializable {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public List<PersonApplyLoan> personLoan(List<Person> persons, int blockId, GeneratorConfiguration conf) {
        random.setSeed(blockId);
        List<PersonApplyLoan> personApplyLoans = new ArrayList<>();

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            LoanGenerator loanGenerator = new LoanGenerator(conf);

            if (loan()) {
                PersonApplyLoan personApplyLoan = PersonApplyLoan.createPersonApplyLoan(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    p,
                    loanGenerator.generateLoan());
                personApplyLoans.add(personApplyLoan);
            }
        }
        return personApplyLoans;
    }


    private boolean loan() {
        //TODO determine whether to generate loan
        return true;
    }

}
