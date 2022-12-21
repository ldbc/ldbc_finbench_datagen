package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.GeometricDistribution;

public class PersonNameDictionary {

    private static final String SEPARATOR = ",";
    private TreeMap<Long,String> personSurnames;
    private GeometricDistribution geometricDistribution;

    //TODO add other params

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    getClass().getResourceAsStream(filePath), "UTF-8");
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumSurnames = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String surname = data[2].trim();
                this.personSurnames.put(totalNumSurnames,surname);
                totalNumSurnames++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PersonNameDictionary() {
        this.personSurnames = new TreeMap<>();
        load(DatagenParams.personSurnameFile);
    }

    public String getGeoDistRandomName(Random random, int numNames) {
        long nameIndex = -1;
        double prob = random.nextDouble();
        int rank = geometricDistribution.inverseFunctionInt(prob);

        if (numNames > rank) {
            nameIndex = rank;
        } else {
            nameIndex = random.nextInt(numNames);
        }

        return personSurnames.get(nameIndex);
    }

    public String getPersonSurname(long k) {
        return personSurnames.get(k);
    }

    public int getNumNames() {
        return personSurnames.size();
    }

}
