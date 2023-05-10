package ldbc.finbench.datagen.generators;

import java.util.Map;
import java.util.Random;
import ldbc.finbench.datagen.generator.DatagenContext;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.PowerLawDegreeDistribution;
import ldbc.finbench.datagen.generator.distribution.TimeDistribution;
import ldbc.finbench.datagen.util.ConfigParser;
import ldbc.finbench.datagen.util.GeneratorConfiguration;
import org.junit.Test;

public class DistributionTest {
    Map<String, String> config;

    public DistributionTest() {
        config = ConfigParser.readConfig("src/main/resources/parameters/params_default.ini");
        config.putAll(ConfigParser.scaleFactorConf("0.1")); // use scale factor 0.1
        DatagenContext.initialize(new GeneratorConfiguration(config));
    }

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

    @Test
    public void testPowerLawDegreeDistribution() {
        PowerLawDegreeDistribution pldd = new PowerLawDegreeDistribution();
        pldd.initialize();
        System.out.println("Generated InDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(pldd.nextInDegree() + " ");
        }
        System.out.println("\nGenerated OutDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(pldd.nextOutDegree() + " ");
        }
        System.out.println("\nGenerated Degrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(pldd.nextDegree() + " ");
        }
        System.out.println();
    }
}
