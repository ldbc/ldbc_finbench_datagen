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

package ldbc.finbench.datagen.generation.generators;

final class ActivityIdGenerator {
    static final int BUCKET_BITS = 14;
    static final int BLOCK_BITS = 20;
    static final int LOCAL_ID_BITS = 28;

    private static final long BUCKET_MASK = (1L << BUCKET_BITS) - 1;
    private static final long BLOCK_MASK = (1L << BLOCK_BITS) - 1;
    private static final long LOCAL_ID_MASK = (1L << LOCAL_ID_BITS) - 1;
    private static final int BLOCK_SHIFT = LOCAL_ID_BITS;
    private static final int BUCKET_SHIFT = BLOCK_BITS + LOCAL_ID_BITS;
    private static final long PERSON_MASK = 1L << 62;
    private static final long POSITIVE_MASK = Long.MAX_VALUE;
    private static final long MIX_MULTIPLIER_1 = 0x7fb5d329728ea185L;
    private static final long MIX_MULTIPLIER_2 = 0x81dadef4bc2dd44dL;

    private ActivityIdGenerator() {
    }

    static long compose(long localId, long bucket, String ownerType, int blockId) {
        checkRange("localId", localId, LOCAL_ID_MASK);
        checkRange("bucket", bucket, BUCKET_MASK);
        checkRange("blockId", blockId, BLOCK_MASK);

        long ownerMask;
        if ("person".equals(ownerType)) {
            ownerMask = PERSON_MASK;
        } else if ("company".equals(ownerType)) {
            ownerMask = 0;
        } else {
            throw new IllegalArgumentException("Unsupported owner type: " + ownerType);
        }

        long rawId = ownerMask | (bucket << BUCKET_SHIFT) | ((long) blockId << BLOCK_SHIFT) | localId;
        return mixPositive(rawId);
    }

    private static long mixPositive(long value) {
        long mixed = value & POSITIVE_MASK;
        mixed ^= mixed >>> 31;
        mixed = (mixed * MIX_MULTIPLIER_1) & POSITIVE_MASK;
        mixed ^= mixed >>> 27;
        mixed = (mixed * MIX_MULTIPLIER_2) & POSITIVE_MASK;
        mixed ^= mixed >>> 33;
        return mixed & POSITIVE_MASK;
    }

    private static void checkRange(String name, long value, long maxValue) {
        if (value < 0 || value > maxValue) {
            throw new IllegalArgumentException(name + " must be between 0 and " + maxValue + ", but was " + value);
        }
    }
}
