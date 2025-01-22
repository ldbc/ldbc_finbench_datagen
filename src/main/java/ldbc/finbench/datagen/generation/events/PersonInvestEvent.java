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

package ldbc.finbench.datagen.generation.events;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.entities.edges.PersonInvestCompany;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class PersonInvestEvent implements Serializable {
    private final RandomGeneratorFarm randomFarm;
    private final Random randIndex;

    public PersonInvestEvent() {
        randomFarm = new RandomGeneratorFarm();
        randIndex = new Random(DatagenParams.defaultSeed);
    }

    public void resetState(int seed) {
        randomFarm.resetRandomGenerators(seed);
        randIndex.setSeed(seed);
    }

    public List<Company> personInvestPartition(List<Person> investors, List<Company> targets) {
        Random numInvestorsRand = randomFarm.get(RandomGeneratorFarm.Aspect.NUMS_PERSON_INVEST);
        Random chooseInvestorRand = randomFarm.get(RandomGeneratorFarm.Aspect.CHOOSE_PERSON_INVESTOR);
        for (Company target : targets) {
            int numInvestors = numInvestorsRand.nextInt(
                DatagenParams.maxInvestors - DatagenParams.minInvestors + 1
            ) + DatagenParams.minInvestors;
            for (int i = 0; i < numInvestors; i++) {
                int index = chooseInvestorRand.nextInt(investors.size());
                Person investor = investors.get(index);
                if (cannotInvest(investor, target)) {
                    continue;
                }
                PersonInvestCompany.createPersonInvestCompany(randomFarm, investor, target);
            }
        }
        return targets;
    }

    public boolean cannotInvest(Person investor, Company target) {
        return target.hasInvestedBy(investor);
    }
}
