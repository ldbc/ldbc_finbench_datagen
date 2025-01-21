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

package ldbc.finbench.datagen.generation.dictionary;

// Private class used to sort countries by their z-order value.
class PlaceZOrder implements Comparable<PlaceZOrder> {

    public int id;
    Integer zvalue;

    PlaceZOrder(int id, int zvalue) {
        this.id = id;
        this.zvalue = zvalue;
    }

    public int compareTo(PlaceZOrder obj) {
        return zvalue.compareTo(obj.zvalue);
    }
}
