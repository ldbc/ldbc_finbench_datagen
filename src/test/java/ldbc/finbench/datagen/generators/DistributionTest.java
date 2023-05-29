package ldbc.finbench.datagen.generators;

import java.util.Map;
import java.util.Random;
import ldbc.finbench.datagen.config.ConfigParser;
import ldbc.finbench.datagen.config.DatagenConfiguration;
import ldbc.finbench.datagen.generation.DatagenContext;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.generation.distribution.PowerLawFormulaDistribution;
import ldbc.finbench.datagen.generation.distribution.TimeDistribution;
import org.junit.Test;

public class DistributionTest {
    Map<String, String> config;

    public DistributionTest() {
        config = ConfigParser.readConfig("src/main/resources/params_default.ini");
        config.putAll(ConfigParser.scaleFactorConf("", "0.1")); // use scale factor 0.1
        DatagenContext.initialize(new DatagenConfiguration(config));
    }

    @Test
    public void testTimeDistribution() {
        TimeDistribution timeDistribution = new TimeDistribution(DatagenParams.hourDistributionFile);
        System.out.println("Hour distribution:");
        for (Map.Entry<Integer, Double> entry : timeDistribution.getHourDistribution().entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println("Generated hours:");
        Random random = new Random(DatagenParams.defaultSeed);
        for (int i = 0; i < 100; i++) {
            System.out.print(timeDistribution.nextHour(random) + " ");
        }
        System.out.println();
    }

    @Test
    public void testPowerLawDegreeDistribution() {
        PowerLawFormulaDistribution inDegreeDist =
            new PowerLawFormulaDistribution(DatagenParams.inDegreeRegressionFile, DatagenParams.tsfMinNumDegree,
                                            DatagenParams.tsfMaxNumDegree);
        inDegreeDist.initialize();
        System.out.println("\nGenerated InDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(inDegreeDist.nextDegree() + " ");
        }
        System.out.println();

        PowerLawFormulaDistribution outDegreeDist =
            new PowerLawFormulaDistribution(DatagenParams.outDegreeRegressionFile, DatagenParams.tsfMinNumDegree,
                                            DatagenParams.tsfMaxNumDegree);
        outDegreeDist.initialize();
        System.out.println("\nGenerated OutDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(outDegreeDist.nextDegree() + " ");
        }
        System.out.println();

        PowerLawFormulaDistribution multiplicity =
            new PowerLawFormulaDistribution(DatagenParams.multiplictyPowerlawRegFile,
                                            DatagenParams.tsfMinMultiplicity,
                                            DatagenParams.tsfMaxMultiplicity);
        multiplicity.initialize();
        System.out.println("\nGenerated Multiplicity:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(multiplicity.nextDegree() + " ");
        }
        System.out.println();
    }
}
