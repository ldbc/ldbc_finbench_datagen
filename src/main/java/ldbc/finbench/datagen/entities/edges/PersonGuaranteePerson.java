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
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonGuaranteePerson implements DynamicActivity, Serializable {
    private final long fromPersonId;
    private final long toPersonId;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String relationship;
    private final String comment;

    public PersonGuaranteePerson(Person fromPerson, Person toPerson,
                                 long creationDate, long deletionDate, boolean isExplicitlyDeleted, String relation,
                                 String comment) {
        this.fromPersonId = fromPerson.getPersonId();
        this.toPersonId = toPerson.getPersonId();
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.relationship = relation;
        this.comment = comment;
    }

    public static void createPersonGuaranteePerson(RandomGeneratorFarm farm, Person fromPerson, Person toPerson) {
        long creationDate = Dictionaries.dates.randomPersonToPersonDate(
            farm.get(RandomGeneratorFarm.Aspect.PERSON_GUARANTEE_DATE), fromPerson, toPerson);
        String relation = Dictionaries.guaranteeRelationships.getDistributedText(
            farm.get(RandomGeneratorFarm.Aspect.PERSON_GUARANTEE_RELATIONSHIP));
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        PersonGuaranteePerson personGuaranteePerson =
            new PersonGuaranteePerson(fromPerson, toPerson, creationDate, 0, false, relation, comment);
        fromPerson.getGuaranteeSrc().add(personGuaranteePerson);
    }

    public long getFromPersonId() {
        return fromPersonId;
    }

    public long getToPersonId() {
        return toPersonId;
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

    public String getRelationship() {
        return relationship;
    }

    public String getComment() {
        return comment;
    }
}
