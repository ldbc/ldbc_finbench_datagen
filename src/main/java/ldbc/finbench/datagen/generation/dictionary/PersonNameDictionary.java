package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generation.DatagenParams;

public class PersonNameDictionary {
    private final TreeMap<Long, String> personSurnames;

    public PersonNameDictionary(String filePath, String separator) {
        this.personSurnames = new TreeMap<>();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumSurnames = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(separator);
                String surname = data[1].trim();
                this.personSurnames.put(totalNumSurnames, surname);
                totalNumSurnames++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandName(Random random) {
        long nameIndex = random.nextInt(personSurnames.size());
        return personSurnames.get(nameIndex);
    }
}
