package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;

public class AccountTypeDictionary implements Serializable {

    private static final String SEPARATOR = ",";

    private TreeMap<Long, String> accountTypes;

    public AccountTypeDictionary() {
        this.accountTypes = new TreeMap<>();
        load(DatagenParams.accountFile);
    }


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
                this.accountTypes.put(totalNumAccounts, accountType);
                totalNumAccounts++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandomType(Random random, int numNames) {
        long nameIndex = random.nextInt(accountTypes.size());
        return accountTypes.get(nameIndex);
    }

    public String getAccountType(long k) {
        return accountTypes.get(k);
    }

    public int getNumNames() {
        return accountTypes.size();
    }

}
