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
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonOwnAccount implements DynamicActivity, Serializable {
    private final long personId;
    private final long accountId;
    private final Account account; // TODO: can be removed
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public PersonOwnAccount(Person person, Account account, long creationDate, long deletionDate,
                            boolean isExplicitlyDeleted, String comment) {
        this.personId = person.getPersonId();
        this.accountId = account.getAccountId();
        this.account = account; // TODO: can be removed
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createPersonOwnAccount(RandomGeneratorFarm farm, Person person, Account account,
                                              long creationDate) {
        // Delete when account is deleted
        account.setOwnerType(PersonOrCompany.PERSON);
        account.setPersonOwner(person);
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        PersonOwnAccount personOwnAccount = new PersonOwnAccount(person, account, creationDate,
                                                                 account.getDeletionDate(),
                                                                 account.isExplicitlyDeleted(),
                                                                 comment);
        person.getPersonOwnAccounts().add(personOwnAccount);
    }

    public long getPersonId() {
        return personId;
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

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public String getComment() {
        return comment;
    }

    public Account getAccount() {
        return account;
    }
}
