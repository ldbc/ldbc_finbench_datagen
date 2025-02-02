/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class SignIn implements DynamicActivity, Serializable {
    private final long mediumId;
    private final long accountId;
    private final long creationDate;
    private final long deletionDate;
    private int countryId;
    private int cityId;
    private final long multiplicityId;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public SignIn(Medium medium, Account account, int mid, long creationDate, long deletionDate,
                  boolean isExplicitlyDeleted, String comment) {
        this.mediumId = medium.getMediumId();
        this.accountId = account.getAccountId();
        this.multiplicityId = mid;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createSignIn(RandomGeneratorFarm farm, int mid, Medium medium, Account account) {
        long creationDate =
            Dictionaries.dates.randomMediumToAccountDate(farm.get(RandomGeneratorFarm.Aspect.SIGNIN_DATE), medium,
                                                         account, account.getDeletionDate());
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
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
    }

    public long getMediumId() {
        return mediumId;
    }

    public long getAccountId() {
        return accountId;
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
