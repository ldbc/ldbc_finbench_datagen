package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;

public class CommonTextDictionary {
    private final TreeMap<Long, String> resources;

    public CommonTextDictionary(String filePath, String separator) {
        this.resources = new TreeMap<>();

        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
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
        long nameIndex = random.nextInt(resources.size());
        return resources.get(nameIndex);
    }
}
