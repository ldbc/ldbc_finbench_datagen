package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generation.DatagenParams;

public class BusinessTypeDictionary implements Serializable {

    private static final String SEPARATOR = ",";

    private TreeMap<Long, String> businessTypes;

    public BusinessTypeDictionary() {
        this.businessTypes = new TreeMap<>();
        load(DatagenParams.businessTypeFile);
    }


    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumBusinessTypes = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String accountType = data[0].trim();
                this.businessTypes.put(totalNumBusinessTypes, accountType);
                totalNumBusinessTypes++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandomType(Random random) {
        long nameIndex = random.nextInt(businessTypes.size());
        return businessTypes.get(nameIndex);
    }
}
