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

public class CompanyInvestCompany implements DynamicActivity, Serializable {
    private final long fromCompanyId;
    private final long toCompanyId;
    private double ratio;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public CompanyInvestCompany(Company fromCompany, Company toCompany,
                                long creationDate, long deletionDate, double ratio, boolean isExplicitlyDeleted,
                                String comment) {
        this.fromCompanyId = fromCompany.getCompanyId();
        this.toCompanyId = toCompany.getCompanyId();
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.ratio = ratio;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createCompanyInvestCompany(RandomGeneratorFarm farm,
                                                  Company investor, Company target) {
        Random dateRandom = farm.get(RandomGeneratorFarm.Aspect.COMPANY_INVEST_DATE);
        long creationDate = Dictionaries.dates.randomCompanyToCompanyDate(dateRandom, investor, target);
        double ratio = farm.get(RandomGeneratorFarm.Aspect.INVEST_RATIO).nextDouble();
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        CompanyInvestCompany companyInvestCompany =
            new CompanyInvestCompany(investor, target, creationDate, 0, ratio, false, comment);
        target.getCompanyInvestCompanies().add(companyInvestCompany);
    }

    public void scaleRatio(double sum) {
        this.ratio = this.ratio / sum;
    }

    public double getRatio() {
        return ratio;
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

    public String getComment() {
        return comment;
    }
}
