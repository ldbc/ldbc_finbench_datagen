package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generation.DatagenParams;

public class LoanUsageDictionary implements Serializable {

    private static final String SEPARATOR = ",";

    private final TreeMap<Long, String> loanUsages;

    public LoanUsageDictionary() {
        this.loanUsages = new TreeMap<>();
        load(DatagenParams.accountFile);
    }


    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumLoanUsage = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String accountType = data[0].trim();
                this.loanUsages.put(totalNumLoanUsage, accountType);
                totalNumLoanUsage++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUniformDistRandomUsage(Random random) {
        long nameIndex = random.nextInt(loanUsages.size());
        return loanUsages.get(nameIndex);
    }
}
