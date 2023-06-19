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

import org.junit.Test;
import static org.junit.Assert.*;


/**
 * Tests for {@link TextSymbolizer}.
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.5
 * @since   1.5
 */
public final class TextSymbolizerTest extends StyleTestCase {
    /**
     * Creates a new test case.
     */
    public TextSymbolizerTest() {
    }

    /**
     * Test of {@code Label} property.
     */
    @Test
    public void testLabel() {
        TextSymbolizer cdt = new TextSymbolizer();

        // Check default
        assertNull(cdt.getLabel());

        // Check get/set
        var value = FF.literal("A random label");
        cdt.setLabel(value);
        assertEquals(value, cdt.getLabel());
    }

    /**
     * Test of {@code Font} property.
     */
    @Test
    public void testFont() {
        TextSymbolizer cdt = new TextSymbolizer();

        // Check default
        Font value = cdt.getFont();
        assertLiteralEquals("normal", value.getStyle());

        // Check get/set
        value = new Font();
        value.setStyle(FF.literal("italic"));
        cdt.setFont(value);
        assertEquals(value, cdt.getFont());
    }

    /**
     * Test of {@code LabelPlacement} property.
     */
    @Test
    public void testLabelPlacement() {
        TextSymbolizer cdt = new TextSymbolizer();

        // Check default
        LabelPlacement value = cdt.getLabelPlacement();
        assertNotNull(value);

        // Check get/set
        value = new PointPlacement();
        cdt.setLabelPlacement(value);
        assertEquals(value, cdt.getLabelPlacement());
    }

    /**
     * Test of {@code Halo} property.
     */
    @Test
    public void testHalo() {
        TextSymbolizer cdt = new TextSymbolizer();

        // Check default
        assertEmpty(cdt.getHalo());

        // Check get/set
        Halo value = new Halo();
        cdt.setHalo(value);
        assertOptionalEquals(value, cdt.getHalo());
    }

    /**
     * Test of {@code Fill} property.
     */
    @Test
    public void testFill() {
        TextSymbolizer cdt = new TextSymbolizer();

        // Check default
        assertNotNull(cdt.getFill());

        // Check get/set
        Fill value = new Fill();
        cdt.setFill(value);
        assertEquals(value, cdt.getFill());
    }
}
