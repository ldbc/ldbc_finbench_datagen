package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.GeometricDistribution;

public class AccountTypeDictionary {

    private static final String SEPARATOR = ",";

    private TreeMap<Long,String> accountType;
    private GeometricDistribution geometricDistribution;

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumAccounts = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String accountType = data[0].trim();
                this.accountType.put(totalNumAccounts,accountType);
                totalNumAccounts++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public AccountTypeDictionary() {
        this.accountType = new TreeMap<>();
        load(DatagenParams.accountFile);
        geometricDistribution = new GeometricDistribution(1);
    }

    public String getGeoDistRandomType(Random random, int numNames) {
        long nameIndex = -1;
        double prob = random.nextDouble();
        int rank = geometricDistribution.inverseFunctionInt(prob);

        if (numNames > rank) {
            nameIndex = rank;
        } else {
            nameIndex = random.nextInt(numNames);
        }

        return accountType.get(nameIndex);
    }

    public String getAccountType(long k) {
        return accountType.get(k);
    }

    public int getNumNames() {
        return accountType.size();
    }

}
