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
package org.apache.sis.internal.style;

import java.util.List;
import org.junit.Test;
import org.apache.sis.metadata.iso.citation.DefaultOnlineResource;

import static org.junit.Assert.*;

// Branch-dependent imports
import org.opengis.feature.Feature;
import org.opengis.filter.Filter;


/**
 * Tests for {@link Rule}.
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.5
 * @since   1.5
 */
public final class RuleTest extends StyleTestCase {
    /**
     * Creates a new test case.
     */
    public RuleTest() {
    }

    /**
     * Test of {@code Name} property.
     */
    @Test
    public void testGetName() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEmpty(cdt.getName());

        // Check get/set
        String value = "A random name";
        cdt.setName(value);
        assertOptionalEquals(value, cdt.getName());
    }

    /**
     * Test of {@code Description} property.
     */
    @Test
    public void testDescription() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEmpty(cdt.getDescription());

        // Check get/set
        Description desc = anyDescription();
        cdt.setDescription(desc);
        assertOptionalEquals(desc, cdt.getDescription());
    }

    /**
     * Test of {@code Legend} property.
     */
    @Test
    public void testLegend() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEmpty(cdt.getLegend());

        // Check get/set
        GraphicLegend value = new GraphicLegend();
        cdt.setLegend(value);
        assertOptionalEquals(value, cdt.getLegend());
    }

    /**
     * Test of {@code Filter} property.
     */
    @Test
    public void testFilter() {
        var cdt = new Rule<Feature>();

        // Check defaults
        assertEquals(Filter.include(), cdt.getFilter());

        // Check get/set
        var value = FF.equal(FF.literal("A"), FF.literal("B"));
        cdt.setFilter(value);
        assertEquals(value, cdt.getFilter());
    }

    /**
     * Test of {@code ElseFilter} property.
     */
    @Test
    public void testIsElseFilter() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertFalse(cdt.isElseFilter());

        // Check get/set
        cdt.setElseFilter(true);
        assertTrue(cdt.isElseFilter());
    }

    /**
     * Test of {@code MinScaleDenominator} property.
     */
    @Test
    public void testMinScaleDenominator() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEquals(0.0, cdt.getMinScaleDenominator(), 0.0);

        // Check get/set
        cdt.setMinScaleDenominator(10.0);
        assertEquals(10.0, cdt.getMinScaleDenominator(), 0.0);
    }

    /**
     * Test of {@code MaxScaleDenominator} property.
     */
    @Test
    public void testGetMaxScaleDenominator() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEquals(Double.POSITIVE_INFINITY, cdt.getMaxScaleDenominator(), 0.0);

        // Check get/set
        cdt.setMaxScaleDenominator(10.0);
        assertEquals(10.0, cdt.getMaxScaleDenominator(), 0.0);
    }

    /**
     * Test of {@code Symbolizer} property.
     */
    @Test
    public void testSymbolizers() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertTrue(cdt.symbolizers().isEmpty());

        // Check get/set
        var value = new LineSymbolizer();
        cdt.symbolizers().add(value);
        assertEquals(List.of(value), cdt.symbolizers());
    }

    /**
     * Test of {@code OnlineSource} property.
     */
    @Test
    public void testGetOnlineSource() {
        var cdt = new Rule<Object>();

        // Check defaults
        assertEmpty(cdt.getOnlineSource());

        // Check get/set
        var r = new DefaultOnlineResource();
        r.setProtocol("HTTP");
        cdt.setOnlineSource(r);
        assertOptionalEquals(r, cdt.getOnlineSource());
    }
}
