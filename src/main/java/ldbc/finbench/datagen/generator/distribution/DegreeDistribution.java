package ldbc.finbench.datagen.generator.distribution;

public abstract class DegreeDistribution {

    public abstract void initialize();

    public abstract void reset(long seed);

    public abstract long nextDegree();

    public double mean(long numPersons) {
        return -1;
    }
}
