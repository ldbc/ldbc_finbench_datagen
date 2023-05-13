package ldbc.finbench.datagen.generation.distribution;

public class GeometricDistribution {
    private double prob;
    private double vprob;

    public GeometricDistribution(double p) {
        setProb(p);
    }

    public double probability(int x) {
        return probability(prob, x);
    }

    public static double probability(double p, int x) {
        if (p < 0 || p > 1) {
            throw new IllegalArgumentException("p not in [0,1]");
        }
        if (p == 0 || p == 1) {
            return 0;
        }
        if (x < 0) {
            return 0;
        }

        return p * Math.pow(1 - p, x);
    }

    public double cdf(int x) {
        return cdf(prob, x);
    }

    public static double cdf(double p, int x) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("p not in [0,1]");
        }
        if (p == 1.0) {
            return 1.0;
        }
        if (p == 0.0) {
            return 0.0;
        }
        if (x < 0) {
            return 0.0;
        }

        return 1.0 - Math.pow(1.0 - p, (double) x + 1.0);
    }

    public double complementaryFunction(int x) {
        return complementaryFunction(prob, x);
    }

    public static double complementaryFunction(double p, int x) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("p not in [0,1]");
        }
        if (x < 0) {
            return 1.0;
        }
        if (p >= 1.0) {                 // In fact, p == 1
            return 0.0;
        }
        if (p <= 0.0) {                 // In fact, p == 0
            return 1.0;
        }

        return Math.pow(1.0 - p, x);
    }

    public int inverseFunctionInt(double u) {
        if (u > 1.0 || u < 0.0) {
            throw new IllegalArgumentException("u not in [0,1]");
        }
        if (prob >= 1.0) {
            return 0;
        }
        if (u <= prob) {
            return 0;
        }
        if (u == 1.0 || prob <= 0.0) {
            return Integer.MAX_VALUE;
        }

        return (int) Math.floor(Math.log1p(-u) / vprob);
    }

    public static int inverseFunction(double p, double u) {
        if (p > 1.0 || p < 0.0) {
            throw new IllegalArgumentException("p not in [0,1]");
        }
        if (u > 1.0 || u < 0.0) {
            throw new IllegalArgumentException("u not in [0,1]");
        }
        if (p == 1.0) {
            return 0;
        }
        if (u <= p) {
            return 0;
        }
        if (u == 1.0 || p == 0.0) {
            return Integer.MAX_VALUE;
        }

        double v = Math.log1p(-p);
        return (int) Math.floor(Math.log1p(-u) / v);
    }

    public double getMean() {
        return GeometricDistribution.getMean(prob);
    }

    public static double getMean(double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("p not in [0,1]");
        }

        return (1 - p) / p;
    }

    public double getVariance() {
        return GeometricDistribution.getVariance(prob);
    }

    public static double getVariance(double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("p not in [0,1]");
        }

        return ((1 - p) / (p * p));
    }

    public double getStandardDeviation() {
        return GeometricDistribution.getStandardDeviation(prob);
    }

    public static double getStandardDeviation(double p) {
        return Math.sqrt(GeometricDistribution.getVariance(p));
    }

    public double getProb() {
        return prob;
    }

    public void setProb(double prob) {
        if (prob < 0 || prob > 1) {
            throw new IllegalArgumentException("p not in [0,1]");
        }
        vprob = Math.log1p(-prob);
        this.prob = prob;
    }

    public double[] getParams() {
        double[] re = {prob};
        return re;
    }

    public String toString() {
        return getClass().getSimpleName() + " : p = " + prob;
    }

}
