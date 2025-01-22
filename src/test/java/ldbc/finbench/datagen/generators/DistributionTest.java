/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            new PowerLawFormulaDistribution(DatagenParams.inDegreeRegressionFile, DatagenParams.transferMinDegree,
                                            DatagenParams.transferMaxDegree);
        inDegreeDist.initialize();
        System.out.println("\nGenerated InDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(inDegreeDist.nextDegree() + " ");
        }
        System.out.println();

        PowerLawFormulaDistribution outDegreeDist =
            new PowerLawFormulaDistribution(DatagenParams.outDegreeRegressionFile, DatagenParams.transferMinDegree,
                                            DatagenParams.transferMaxDegree);
        outDegreeDist.initialize();
        System.out.println("\nGenerated OutDegrees:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(outDegreeDist.nextDegree() + " ");
        }
        System.out.println();

        PowerLawFormulaDistribution multiplicity =
            new PowerLawFormulaDistribution(DatagenParams.multiplictyPowerlawRegFile,
                                            DatagenParams.transferMinMultiplicity,
                                            DatagenParams.transferMaxMultiplicity);
        multiplicity.initialize();
        System.out.println("\nGenerated Multiplicity:");
        for (int i = 0; i < 1000; i++) {
            System.out.print(multiplicity.nextDegree() + " ");
        }
        System.out.println();
    }
}
