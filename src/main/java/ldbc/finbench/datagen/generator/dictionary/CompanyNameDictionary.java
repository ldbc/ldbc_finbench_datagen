package ldbc.finbench.datagen.generator.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.generator.distribution.GeometricDistribution;
import org.apache.spark.util.random.GapSamplingReplacement;

public class CompanyNameDictionary {

    private static final String SEPARATOR = ",";
    private TreeMap<Long,String> companyNames;
    private GeometricDistribution geometricDistribution;

    //TODO add other params

    private void load(String filePath) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(
                    getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
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
        geometricDistribution = new GeometricDistribution(1);
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

        return companyNames.get(nameIndex);
    }

    public String getCompanyName(long k) {
        return companyNames.get(k);
    }

    public int getNumNames() {
        return companyNames.size();
    }

}
