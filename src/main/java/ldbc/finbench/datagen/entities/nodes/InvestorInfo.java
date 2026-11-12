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

package ldbc.finbench.datagen.entities.nodes;

import java.io.Serializable;

/**
 * Lightweight representation of a Person or Company for broadcast in invest events.
 * Only contains the fields actually used by edge creation: id and creationDate.
 */
public class InvestorInfo implements Serializable {
    private final long id;
    private final long creationDate;

    public InvestorInfo(long id, long creationDate) {
        this.id = id;
        this.creationDate = creationDate;
    }

    public long getId() {
        return id;
    }

    public long getCreationDate() {
        return creationDate;
    }
}
