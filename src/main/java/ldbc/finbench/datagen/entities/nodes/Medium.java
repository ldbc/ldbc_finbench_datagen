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
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.entities.edges.SignIn;

public class Medium implements Serializable {
    private long mediumId;
    private String mediumName;
    private final List<SignIn> signIns;
    private long creationDate;
    private boolean isBlocked;
    private long lastLogin;
    private String riskLevel;

    public Medium() {
        signIns = new ArrayList<>();
    }

    public Medium(long mediumId, String mediumName, long creationDate, boolean isBlocked) {
        signIns = new ArrayList<>();
        this.mediumId = mediumId;
        this.mediumName = mediumName;
        this.creationDate = creationDate;
        this.isBlocked = isBlocked;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Medium) {
            Medium other = (Medium) obj;
            return mediumId == other.mediumId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(mediumId);
    }

    public long getMediumId() {
        return mediumId;
    }

    public void setMediumId(long mediumId) {
        this.mediumId = mediumId;
    }

    public String getMediumName() {
        return mediumName;
    }

    public void setMediumName(String mediumName) {
        this.mediumName = mediumName;
    }

    public List<SignIn> getSignIns() {
        return signIns;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

    public long getLastLogin() {
        return lastLogin;
    }

    public void setLastLogin(long lastLogin) {
        this.lastLogin = lastLogin;
    }

    public String getRiskLevel() {
        return riskLevel;
    }

    public void setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
    }
}
