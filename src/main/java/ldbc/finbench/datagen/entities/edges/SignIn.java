package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SignIn implements DynamicActivity, Serializable {
    private final Medium medium;
    private final Account account;
    private final long creationDate;
    private final long deletionDate;
    private int countryId;
    private int cityId;
    private final long multiplicityId;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public SignIn(Medium medium, Account account, int mid, long creationDate, long deletionDate,
                  boolean isExplicitlyDeleted, String comment) {
        this.medium = medium;
        this.account = account;
        this.multiplicityId = mid;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static SignIn createSignIn(RandomGeneratorFarm farm, int mid, Medium medium, Account account) {
        long creationDate =
            Dictionaries.dates.randomMediumToAccountDate(farm.get(RandomGeneratorFarm.Aspect.SIGNIN_DATE), medium,
                                                         account, account.getDeletionDate());
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComment(farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        SignIn signIn = new SignIn(medium, account, mid, creationDate, account.getDeletionDate(),
                                   account.isExplicitlyDeleted(), comment);
        // Set country and city
        int countryId =
            Dictionaries.places.getCountryForPerson(farm.get(RandomGeneratorFarm.Aspect.SIGNIN_COUNTRY));
        signIn.setCountryId(countryId);
        signIn.setCityId(
            Dictionaries.places.getRandomCity(farm.get(RandomGeneratorFarm.Aspect.SIGNIN_CITY), countryId));

        medium.getSignIns().add(signIn);
        account.getSignIns().add(signIn);

        return signIn;
    }

    public Medium getMedium() {
        return medium;
    }

    public Account getAccount() {
        return account;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public long getMultiplicityId() {
        return multiplicityId;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getLocation() {
        return Dictionaries.places.getPlaceName(countryId) + " -> " + Dictionaries.places.getPlaceName(cityId);
    }

    public String getComment() {
        return comment;
    }
}
