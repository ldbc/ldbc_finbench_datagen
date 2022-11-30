package ldbc.finbench.datagen.generator.dictionary;

import ldbc.finbench.datagen.generator.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class MediumNameDictionary {

    private static final String SEPARATOR = ",";

    private TreeMap<Long,String> mediumNames;

    //TODO add other params

    private void load(String filePath){
        try{
            InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8");
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalMediumNames = 0;
            while ((line = dictionary.readLine()) != null){
                String[] data = line.split(SEPARATOR);
                String surname = data[0].trim();
                this.mediumNames.put(totalMediumNames,surname);
                totalMediumNames++;
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MediumNameDictionary(){
        this.mediumNames = new TreeMap<>();
        load(DatagenParams.mediumNameFile);
    }

    public String getMediumName(long k){
        return mediumNames.get(k);
    }

}
