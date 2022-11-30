package ldbc.finbench.datagen.generator.dictionary;

import ldbc.finbench.datagen.generator.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class AccountDictionary {

    //It seems that account don't need any dictionary？

//    private static final String SEPARATOR = ",";
//
//    private TreeMap<Long,Long> createTime;
//
//    private TreeMap<Long,String> accountType;
//
//    private void load(String filePath){
//        try{
//            InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8");
//            BufferedReader dictionary = new BufferedReader(inputStreamReader);
//            String line;
//            long totalNumAccounts = 0;
//            while ((line = dictionary.readLine()) != null){
//                String[] data = line.split(SEPARATOR);
//                long createTime = Long.parseLong(data[0]);
//                this.createTime.put(totalNumAccounts, createTime);
//                String accountType = data[1].trim();
//                this.accountType.put(totalNumAccounts,accountType);
//                totalNumAccounts++;
//            }
//            dictionary.close();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public AccountDictionary() {
//        this.createTime = new TreeMap<>();
//        this.accountType = new TreeMap<>();
//        load(DatagenParams.accountFile);
//    }
//
//    public Long getCreateTime(long k) {
//        return createTime.get(k);
//    }
//
//    public String getAccountType(long k) {
//        return accountType.get(k);
//    }

}
