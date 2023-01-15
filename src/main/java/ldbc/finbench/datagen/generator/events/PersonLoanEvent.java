package ldbc.finbench.datagen.generator.events;

import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonApplyLoan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generator.generators.LoanGenerator;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonLoanEvent {
    private RandomGeneratorFarm randomFarm;
    private Random random;

    public PersonLoanEvent() {
        randomFarm = new RandomGeneratorFarm();
        random = new Random();
    }

    public void personLoan(List<Person> persons, int blockId) {
        random.setSeed(blockId);

        for (int i = 0; i < persons.size(); i++) {
            Person p = persons.get(i);
            LoanGenerator loanGenerator = new LoanGenerator();

            if (loan()) {
                PersonApplyLoan.createPersonApplyLoan(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        p,
                        loanGenerator.generateLoan());
            }
        }
    }

    private boolean loan() {
        //TODO determine whether to generate loan
        return true;
    }

    private boolean deposit() {
        //TODO determine whether to generate deposit
        return true;
    }

    private boolean transfer() {
        //TODO determine whether to generate transfer
        return true;
    }

    private boolean repay() {
        //TODO determine whether to generate repay
        return true;
    }
}
