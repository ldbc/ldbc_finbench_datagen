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
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

public class CommonTextDictionary {
    private final TreeMap<Long, String> resources;

    public CommonTextDictionary(String filePath, String separator) {
        this.resources = new TreeMap<>();

        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                Objects.requireNonNull(getClass().getResourceAsStream(filePath)), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNum = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(separator);
                String surname = data[0].trim();
                this.resources.put(totalNum, surname);
                totalNum++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandomText(Random random) {
        long index = random.nextInt(resources.size());
        return resources.get(index);
    }

    public String getUniformDistRandomTextForComments(Random random) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            text.append(resources.get((long) random.nextInt(resources.size()))).append(" ");
        }
        return text.toString();
    }
}
