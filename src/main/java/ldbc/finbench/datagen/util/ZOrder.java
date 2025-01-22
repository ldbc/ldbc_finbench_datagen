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

package ldbc.finbench.datagen.util;


public class ZOrder {

    private int maxBitNum;

    public ZOrder(int maxNumBit) {
        this.maxBitNum = maxNumBit;
    }

    public int getZValue(int x, int y) {

        String sx = Integer.toBinaryString(x);
        int numberToAddX = maxBitNum - sx.length();
        for (int i = 0; i < numberToAddX; i++) {
            sx = "0" + sx;
        }

        String sy = Integer.toBinaryString(y);
        int numberToAddY = maxBitNum - sy.length();
        for (int i = 0; i < numberToAddY; i++) {
            sy = "0" + sy;
        }

        String sz = "";
        for (int i = 0; i < sx.length(); i++) {
            sz = sz + sx.substring(i, i + 1) + "" + sy.substring(i, i + 1);
        }

        return Integer.parseInt(sz, 2);
    }
}
