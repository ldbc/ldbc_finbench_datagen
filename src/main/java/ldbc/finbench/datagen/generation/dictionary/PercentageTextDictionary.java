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

package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PercentageTextDictionary {
    private final List<String> resources;
    private final List<Double> cumulativeDistribution;

    public PercentageTextDictionary(String filePath, String separator) {
        resources = new ArrayList<>();
        cumulativeDistribution = new ArrayList<>();

        try {
            BufferedReader dictionary = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8"));
            String line;
            double cummulativeDist = 0.0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(separator);
                String browser = data[0];
                cummulativeDist += Double.parseDouble(data[1]);
                resources.add(browser);
                cumulativeDistribution.add(cummulativeDist);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName(int id) {
        return resources.get(id);
    }

    public String getDistributedText(Random random) {
        double prob = random.nextDouble();
        int minIdx = 0;
        int maxIdx = (byte) ((prob < cumulativeDistribution.get(minIdx)) ? minIdx : cumulativeDistribution
            .size() - 1);
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return resources.get(maxIdx);
    }
}
