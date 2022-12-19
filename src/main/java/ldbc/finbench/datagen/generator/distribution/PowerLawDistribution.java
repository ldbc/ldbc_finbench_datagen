package ldbc.finbench.datagen.generator.distribution;

import java.util.Random;

public class PowerLawDistribution {

    private double alpha;
    private double beta;

    public PowerLawDistribution(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
    }

    public int getValue(Random random) {
        return (int)inverseFunction(random.nextDouble());
    }

    public double getDouble(Random random) {
        return inverseFunction(random.nextDouble());
    }

    public static double cdf(double alpha, double beta, double x) {
        return alpha * Math.pow(x,beta);
    }

    public double inverseFunction(double y) {
        return inverseFunction(alpha,beta,y);
    }

    public static double inverseFunction(double alpha, double beta, double y) {
        return Math.pow(y / alpha,1.0 / beta);
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public String toString() {
        return getClass().getSimpleName() + " : alpha = " + alpha + " : beta = " + beta;
    }

}
