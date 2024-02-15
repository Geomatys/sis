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
package org.apache.sis.math;

import org.apache.sis.measure.NumberRange;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestCase;


/**
 * Tests the {@link LinearlyDerivedVector} class.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class LinearlyDerivedVectorTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public LinearlyDerivedVectorTest() {
    }

    /**
     * Tests creation of a vector and a few methods that haven been overridden.
     */
    @Test
    public void testBasicMethods() {
        Vector vec = Vector.create(new int[]    {10, 12, 15, -2}, false).transform(2, -3);
        final double[] expected =  new double[] {17, 21, 27, -7};
        assertArrayEquals(new float[] {17, 21, 27, -7}, vec.floatValues());
        assertArrayEquals(expected, vec.doubleValues());
        assertTrue(vec.equals(Vector.create(expected)));                      // `equals` must be invoked on `vec`.
        assertEquals(NumberRange.create(-7d, true, 27d, true), vec.range());
    }

    /**
     * Tests sub-lists of a derived vector.
     */
    @Test
    public void testSubList() {
        Vector vec = Vector.create(new int[]    {10, 12, 15, -2}, false).transform(2, -3).subList(0, 3);
        final double[] expected =  new double[] {17, 21, 27};
        assertArrayEquals(new float[] {17, 21, 27}, vec.floatValues());
        assertArrayEquals(expected, vec.doubleValues());
        assertTrue  (vec.equals(Vector.create(expected)));
        assertEquals(NumberRange.create(17d, true, 27d, true), vec.range());
        assertNull  (vec.increment(0.9));
        assertEquals(5d, vec.increment(2));
    }

    /**
     * Tests application of two {@code transform} method calls, which should be merged in a single wrapper.
     * This method does not verify that the two method calls have been correctly merged (that would require
     * access to private fields), but we verified using debugger. This method verifies at least that the
     * resulting vector computes correct values.
     */
    @Test
    public void testOptimizations() {
        Vector vec = Vector.create(new int[] {10, 12, 15, -2}, false);
        vec = vec.transform(2, -3);
        vec = vec.transform(-1, 2);
        assertArrayEquals(new float[] {-15, -19, -25, 9}, vec.floatValues());

        vec = vec.subList(0, 2).transform(-1, 0);
        assertArrayEquals(new float[] {15, 19}, vec.floatValues());

        vec = vec.concatenate(Vector.create(new double[] {8, 3})).transform(0.25, 4);
        assertArrayEquals(new float[] {7.75f, 8.75f, 6f, 4.75f}, vec.floatValues());
    }
}
