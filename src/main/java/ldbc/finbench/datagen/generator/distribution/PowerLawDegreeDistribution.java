package ldbc.finbench.datagen.generator.distribution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import ldbc.finbench.datagen.generator.DatagenParams;

// PowerLaw Degree Distribution is generated using transformed uniform distribution.
// The formula is: x = [(x1^(n+1) - x0^(n+1))*y + x0^(n+1)]^(1/(n+1))
// where x0 and x1 are the min and max of the distribution, n is beta, and y is a random number between 0 and 1.
// TODO:
//  This degree generator should generate in-degree and out-degree separately. But it is complex to make the
//  sum of indegree equals to sum of outdegree. Temporarily we use the same beta for in-degree and out-degree.
public class PowerLawDegreeDistribution extends DegreeDistribution implements Serializable {
    private double inDegreeBeta;
    private double outDegreeBeta;
    private long minDegree;
    private long maxDegree;
    private Random degreeRandom;
    private Random inDegreeRandom;
    private Random outDegreeRandom;

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
            String[] valueArray = line.substring(prefix.length()).split(",");
            double sum = Arrays.stream(valueArray).mapToDouble(Double::parseDouble).sum();
            return sum / valueArray.length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize() {
        inDegreeBeta = readAndCalculateMeanForBeta(DatagenParams.inDegreeRegressionFile);
        outDegreeBeta = readAndCalculateMeanForBeta(DatagenParams.outDegreeRegressionFile);
        minDegree = DatagenParams.minNumDegree;
        maxDegree = DatagenParams.maxNumDegree;
        inDegreeRandom = new Random(0);
        outDegreeRandom = new Random(0);
        degreeRandom = new Random(0);
    }

    @Override
    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        inDegreeRandom.setSeed(seedRandom.nextLong());
        outDegreeRandom.setSeed(seedRandom.nextLong());
        degreeRandom.setSeed(seedRandom.nextLong());
    }

    @Override
    public long nextDegree() {
        double beta = (inDegreeBeta + outDegreeBeta) / 2;
        double a = Math.pow(maxDegree, beta + 1);
        double b = Math.pow(minDegree, beta + 1);
        double f = (a - b) * inDegreeRandom.nextDouble() + b;
        return (long) Math.pow(f, 1 / (beta + 1));
    }

    public long nextInDegree() {
        double a = Math.pow(maxDegree, inDegreeBeta + 1);
        double b = Math.pow(minDegree, inDegreeBeta + 1);
        double f = (a - b) * inDegreeRandom.nextDouble() + b;
        return (long) Math.pow(f, 1 / (inDegreeBeta + 1));
    }

    public long nextOutDegree() {
        double a = Math.pow(maxDegree, outDegreeBeta + 1);
        double b = Math.pow(minDegree, outDegreeBeta + 1);
        double f = (a - b) * outDegreeRandom.nextDouble() + b;
        return (long) Math.pow(f, 1 / (outDegreeBeta + 1));
    }
}
