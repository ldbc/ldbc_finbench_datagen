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
import java.util.Random;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class CompanyGuaranteeCompany implements DynamicActivity, Serializable {
    private final long fromCompanyId;
    private final long toCompanyId;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String relationship;
    private final String comment;

    public CompanyGuaranteeCompany(Company fromCompany, Company toCompany,
                                   long creationDate, long deletionDate, boolean isExplicitlyDeleted, String relation,
                                   String comment) {
        this.fromCompanyId = fromCompany.getCompanyId();
        this.toCompanyId = toCompany.getCompanyId();
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.relationship = relation;
        this.comment = comment;
    }

    public static void createCompanyGuaranteeCompany(RandomGeneratorFarm farm, Company fromCompany, Company toCompany) {
        Random dateRand = farm.get(RandomGeneratorFarm.Aspect.COMPANY_GUARANTEE_DATE);
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(dateRand, fromCompany, toCompany);
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        CompanyGuaranteeCompany companyGuaranteeCompany = new CompanyGuaranteeCompany(fromCompany,
                                                                                      toCompany, creationDate, 0, false,
                                                                                      "business associate", comment);
        fromCompany.getGuaranteeSrc().add(companyGuaranteeCompany);
    }

    public long getFromCompanyId() {
        return fromCompanyId;
    }

    public long getToCompanyId() {
        return toCompanyId;
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
