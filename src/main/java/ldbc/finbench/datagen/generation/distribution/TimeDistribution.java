package ldbc.finbench.datagen.generation.distribution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class TimeDistribution {
    private Map<Integer, Double> hourDistribution;
    private double[] hourProbs;
    private final double[] hourCumulatives;
    
    public TimeDistribution(String hourDistributionFile) {
        loadDistribution(hourDistributionFile);
        hourCumulatives = new double[hourProbs.length];
        hourCumulatives[0] = hourProbs[0];
        for (int i = 1; i < hourProbs.length; i++) {
            hourCumulatives[i] = hourCumulatives[i - 1] + hourProbs[i];
        }
    }

    public void loadDistribution(String hourDistributionFile) {
        try {
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(hourDistributionFile), StandardCharsets.UTF_8));
            hourDistribution = new TreeMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(" ");
                hourDistribution.put(Integer.parseInt(data[1]), Double.parseDouble(data[0]));
            }
            reader.close();
            hourProbs = hourDistribution.values().stream().mapToDouble(Double::doubleValue).toArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, Double> getHourDistribution() {
        return hourDistribution;
    }

    public long nextHour(Random random) {
        double rand = random.nextDouble();
        for (int i = 0; i < hourProbs.length; i++) {
            if (rand < hourCumulatives[i]) {
                return i;
            }
        }
        return -1;
    }

    public long nextMinute(Random random) {
        return (long) (random.nextDouble() * 60);
    }

    public long nextSecond(Random random) {
        return (long) (random.nextDouble() * 60);
    }
}
