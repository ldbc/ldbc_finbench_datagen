package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;

public class AccountDictionary {

    private static final String SEPARATOR = ",";

    private TreeMap<Long,String> accountType;

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    getClass().getResourceAsStream(filePath), "UTF-8");
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

    public AccountDictionary() {
        this.accountType = new TreeMap<>();
        load(DatagenParams.accountFile);
    }

    public String getAccountType(long k) {
        return accountType.get(k);
    }

}
