package ldbc.finbench.datagen.generator.dictionary;

import ldbc.finbench.datagen.generator.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class PersonNameDictionary {

    private static final String SEPARATOR = ",";

    private TreeMap<Long,String> personSurnames;

    //TODO add other params

    private void load(String filePath){
        try{
            InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8");
            BufferedReader dictionary = new BufferedReader(inputStreamReader);
            String line;
            long totalNumSurnames = 0;
            while ((line = dictionary.readLine()) != null){
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

    public PersonNameDictionary(){
        this.personSurnames = new TreeMap<>();
        load(DatagenParams.personSurnameFile);
    }

    public String getPersonSurname(long k){
        return personSurnames.get(k);
    }

}
