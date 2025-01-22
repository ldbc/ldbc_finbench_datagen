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


public class EmailDictionary {
    private final List<String> emails;
    private final List<Double> cumulativeDistribution;

    public EmailDictionary(String filePath, String separator) {
        try {
            BufferedReader emailDictionary = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8"));

            emails = new ArrayList<>();
            cumulativeDistribution = new ArrayList<>();

            String line;
            double cummulativeDist = 0.0;
            while ((line = emailDictionary.readLine()) != null) {
                String[] data = line.split(separator);
                emails.add(data[0]);
                if (data.length == 2) {
                    cummulativeDist += Double.parseDouble(data[1]);
                    cumulativeDistribution.add(cummulativeDist);
                }
            }
            emailDictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getRandomEmail(Random randomTop, Random randomEmail) {
        int minIdx = 0;
        int maxIdx = cumulativeDistribution.size() - 1;
        double prob = randomTop.nextDouble();
        if (prob > cumulativeDistribution.get(maxIdx)) {
            int idx = randomEmail.nextInt(emails.size() - cumulativeDistribution.size()) + cumulativeDistribution
                .size();
            return emails.get(idx);
        } else if (prob < cumulativeDistribution.get(minIdx)) {
            return emails.get(minIdx);
        }

        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return emails.get(maxIdx);
    }
}
