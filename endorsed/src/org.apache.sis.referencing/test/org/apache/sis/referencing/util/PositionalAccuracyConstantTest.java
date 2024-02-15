/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sis.referencing.util;

import java.util.Collection;
import org.opengis.metadata.quality.ConformanceResult;
import org.opengis.metadata.quality.Result;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestUtilities;
import org.apache.sis.test.TestCase;


/**
 * Tests the {@link PositionalAccuracyConstant} class.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 */
public final class PositionalAccuracyConstantTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public PositionalAccuracyConstantTest() {
    }

    /**
     * Tests {@link PositionalAccuracyConstant} constants.
     */
    @Test
    public void testPositionalAccuracy() {
        assertEquals(PositionalAccuracyConstant.DATUM_SHIFT_APPLIED,
                     PositionalAccuracyConstant.DATUM_SHIFT_APPLIED);

        assertEquals(PositionalAccuracyConstant.DATUM_SHIFT_OMITTED,
                     PositionalAccuracyConstant.DATUM_SHIFT_OMITTED);

        assertNotSame(PositionalAccuracyConstant.DATUM_SHIFT_APPLIED,
                      PositionalAccuracyConstant.DATUM_SHIFT_OMITTED);

        final Collection<? extends Result> appliedResults = PositionalAccuracyConstant.DATUM_SHIFT_APPLIED.getResults();
        final Collection<? extends Result> omittedResults = PositionalAccuracyConstant.DATUM_SHIFT_OMITTED.getResults();
        final ConformanceResult applied = (ConformanceResult) TestUtilities.getSingleton(appliedResults);
        final ConformanceResult omitted = (ConformanceResult) TestUtilities.getSingleton(omittedResults);
        assertNotSame(applied, omitted);
        assertTrue (applied.pass(), "DATUM_SHIFT_APPLIED");
        assertFalse(omitted.pass(), "DATUM_SHIFT_OMITTED");
        assertNotEquals(applied, omitted);
        assertNotEquals(appliedResults, omittedResults);
        assertNotEquals(PositionalAccuracyConstant.DATUM_SHIFT_APPLIED,
                        PositionalAccuracyConstant.DATUM_SHIFT_OMITTED);
    }
}
