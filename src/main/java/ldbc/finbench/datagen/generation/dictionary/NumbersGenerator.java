package ldbc.finbench.datagen.generation.dictionary;

import java.util.Random;

public class NumbersGenerator {
    public NumbersGenerator() {
    }

    public String generatePhonenum(Random random) {
        return String.format("%03d", random.nextInt(1000))
            + "-" + String.format("%04d", random.nextInt(10000));
    }
}
