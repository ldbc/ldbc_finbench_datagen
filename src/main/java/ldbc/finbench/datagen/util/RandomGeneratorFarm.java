package ldbc.finbench.datagen.util;

import java.io.Serializable;
import java.util.Random;

public class RandomGeneratorFarm implements Serializable {
    private final int numRandomGenerators;
    private final Random[] randomGenerators;

    public enum Aspect {
        DATE,
        GENDER,
        INT,
        PERSON_NAME,
        COMPANY_NAME,
        MEDIUM_NAME,
        ACCOUNT_TYPE,
        ACCOUNT_OWNER_TYPE,
        DELETE_ACCOUNT,
        LOAN_AMOUNT,
        UNIFORM,
        NUM_ASPECT                  // This must be always the last one.
    }

    public RandomGeneratorFarm() {
        numRandomGenerators = Aspect.values().length;
        randomGenerators = new Random[numRandomGenerators];
        for (int i = 0; i < numRandomGenerators; ++i) {
            randomGenerators[i] = new Random();
        }
    }

    public Random get(Aspect aspect) {
        return randomGenerators[aspect.ordinal()];
    }

    public void resetRandomGenerators(long seed) {
        Random seedRandom = new Random(7654321L + 1234567 * seed);
        for (int i = 0; i < numRandomGenerators; i++) {
            randomGenerators[i].setSeed(seedRandom.nextLong());
        }
    }

}
