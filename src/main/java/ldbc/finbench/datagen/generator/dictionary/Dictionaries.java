package ldbc.finbench.datagen.generator.dictionary;

import ldbc.finbench.datagen.generator.generators.DateGenerator;

public class Dictionaries {
    public static PersonNameDictionary personNames = null;
    public static CompanyNameDictionary companyNames = null;
    public static MediumNameDictionary mediumNames = null;
    public static AccountDictionary accounts = null;
    public static DateGenerator dates = null;

    public static void loadDictionaries() {
        personNames = new PersonNameDictionary();
        companyNames = new CompanyNameDictionary();
        mediumNames = new MediumNameDictionary();
        accounts = new AccountDictionary();
        dates = new DateGenerator();
    }
}
