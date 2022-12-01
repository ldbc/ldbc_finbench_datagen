package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;

public class CompanyNameDictionary {

    private static final String SEPARATOR = ",";

    private TreeMap<Long,String> companyNames;

    //TODO add other params

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    getClass().getResourceAsStream(filePath), "UTF-8");
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumCompanies = 0;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String companyName = data[0].trim();
                this.companyNames.put(totalNumCompanies,companyName);
                totalNumCompanies++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompanyNameDictionary() {
        this.companyNames = new TreeMap<>();
        load(DatagenParams.companyNameFile);
    }

    public String getCompanyName(long k) {
        return companyNames.get(k);
    }

}
