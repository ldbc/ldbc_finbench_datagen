package ldbc.finbench.datagen.generator.distribution;

import java.util.Random;
import umontreal.iro.lecuyer.probdist.PowerDist;

public class PowerLawDistribution {

    private PowerDist powerDist;

    public PowerLawDistribution(double a, double b, double alpha) {
        powerDist = new PowerDist(a, b, alpha);
    }

    public PowerDist getPowerDist() {
        return powerDist;
    }

    public int getValue(Random random) {
        return (int) powerDist.inverseF(random.nextDouble());
    }

    public double getDouble(Random random) {
        return powerDist.inverseF(random.nextDouble());
    }

}
