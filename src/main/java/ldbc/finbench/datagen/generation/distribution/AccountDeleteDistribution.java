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

package ldbc.finbench.datagen.generation.distribution;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AccountDeleteDistribution implements Serializable {

    private double[] distribution;
    private final String distributionFile;

    public AccountDeleteDistribution(String distributionFile) {
        this.distributionFile = distributionFile;
    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(distributionFile), StandardCharsets.UTF_8));
            List<Double> temp = new ArrayList<>();
            String line;
            while ((line = distributionBuffer.readLine()) != null) {
                Double prob = Double.valueOf(line);
                temp.add(prob);
            }
            distribution = new double[temp.size()];
            int index = 0;
            for (Double ele : temp) {
                distribution[index] = ele;
                ++index;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isDeleted(Random random, long maxDegree) {
        if (maxDegree < distribution.length) {
            return random.nextDouble() < distribution[(int) maxDegree];
        } else {
            // support degree more than 1000
            return random.nextDouble() < Math.pow(0.99, maxDegree);
        }
    }
}
