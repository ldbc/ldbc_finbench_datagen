package ldbc.finbench.datagen.generation.distribution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import ldbc.finbench.datagen.generation.DatagenParams;

// PowerLaw Degree Distribution is generated using transformed uniform distribution.
// The formula is: x = [(x1^(n+1) - x0^(n+1))*y + x0^(n+1)]^(1/(n+1))
// where x0 and x1 are the min and max of the distribution, n is beta, and y is a random number between 0 and 1.
// TODO:
//  This degree generator should generate in-degree and out-degree separately. But it is complex to make the
//  sum of indegree equals to sum of outdegree. Temporarily we use the same beta for in-degree and out-degree.
public class PowerLawFormulaDistribution extends DegreeDistribution implements Serializable {
    private final String degreeDistributionFile;
    private double degreeBeta;
    private final long minDegree;
    private final long maxDegree;
    private Random degreeRandom;

    public PowerLawFormulaDistribution(String filePath, long min, long max) {
        degreeDistributionFile = filePath;
        minDegree = min;
        maxDegree = max;
    }

    public double readAndCalculateMeanForBeta(String filePath) {
        String prefix = "beta:";
        try {
            BufferedReader fbDataReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8));
            String line;
            while ((line = fbDataReader.readLine()) != null) {
                if (line.startsWith(prefix)) {
                    break;
                }
            }
            fbDataReader.close();
            String[] betaArray = line.substring(prefix.length()).split(",");
            double sum = Arrays.stream(betaArray).mapToDouble(Double::parseDouble).sum();
            return sum / betaArray.length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize() {
        degreeBeta = readAndCalculateMeanForBeta(degreeDistributionFile);
        degreeRandom = new Random(DatagenParams.defaultSeed);
    }

    @Override
    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        degreeRandom.setSeed(seedRandom.nextLong());
    }

    @Override
    public long nextDegree() {
        double a = Math.pow(maxDegree, degreeBeta + 1);
        double b = Math.pow(minDegree, degreeBeta + 1);
        double f = (a - b) * degreeRandom.nextDouble() + b;
        return (long) Math.pow(f, 1 / (degreeBeta + 1));
    }
}
