package ldbc.finbench.datagen.generators;

import java.util.Map;
import java.util.Random;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.TimeDistribution;
import org.junit.Test;

public class DistributionTest {
    @Test
    public void testTimeDistribution() {
        TimeDistribution timeDistribution = new TimeDistribution(DatagenParams.hourDistributionFile);
        System.out.println("Hour distribution:");
        for (Map.Entry<Integer, Double> entry : timeDistribution.getHourDistribution().entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println("Generated hours:");
        for (int i = 0; i < 100; i++) {
            System.out.print(timeDistribution.nextHour(new Random()) + " ");
        }
        System.out.println();
    }
}
