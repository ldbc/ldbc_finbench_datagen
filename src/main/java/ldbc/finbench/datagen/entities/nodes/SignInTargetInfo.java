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
 * Lightweight representation of an Account for broadcast in signIn events.
 * Only contains the fields actually used by SignIn edge creation.
 */
public class SignInTargetInfo implements Serializable {
    private final long accountId;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;

    public SignInTargetInfo(long accountId, long creationDate, long deletionDate, boolean isExplicitlyDeleted) {
        this.accountId = accountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public long getAccountId() {
        return accountId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }
}
