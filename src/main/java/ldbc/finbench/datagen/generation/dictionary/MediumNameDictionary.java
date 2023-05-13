package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generation.DatagenParams;

public class MediumNameDictionary {

    private static final String SEPARATOR = ",";
    private final TreeMap<Long, String> mediumNames;

    public MediumNameDictionary() {
        this.mediumNames = new TreeMap<>();
        load(DatagenParams.mediumNameFile);
    }

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalMediumNames = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String surname = data[0].trim();
                this.mediumNames.put(totalMediumNames, surname);
                totalMediumNames++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandomName(Random random) {
        long nameIndex = random.nextInt(mediumNames.size());
        return mediumNames.get(nameIndex);
    }

    public String getMediumName(long k) {
        return mediumNames.get(k);
    }

    public int getNumNames() {
        return mediumNames.size();
    }

}
