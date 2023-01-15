package ldbc.finbench.datagen.util;

import java.util.Random;

public class RandomGeneratorFarm {
    private int numRandomGenerators;
    private Random[] randomGenerators;

    public enum Aspect {
        DATE,
        GENDER,
        INT,
        PERSON_NAME,
        COMPANY_NAME,
        MEDIUM_NAME,
        ACCOUNT_TYPE
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
