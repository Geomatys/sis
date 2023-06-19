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

import java.awt.Color;
import org.junit.Test;


/**
 * Tests for {@link Fill}.
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.5
 * @since   1.5
 */
public final class FillTest extends StyleTestCase {
    /**
     * Creates a new test case.
     */
    public FillTest() {
    }

    /**
     * Test of {@code GraphicFill} property.
     */
    @Test
    public void testGraphicFill() {
        Fill cdt = new Fill();

        // Check default
        assertEmpty(cdt.getGraphicFill());

        // Check get/set
        final GraphicFill value = new GraphicFill();
        cdt.setGraphicFill(value);
        assertOptionalEquals(value, cdt.getGraphicFill());
    }

    /**
     * Test of {@code Color} property.
     */
    @Test
    public void testColor() {
        Fill cdt = new Fill();

        // Check default
        assertLiteralEquals(Color.GRAY, cdt.getColor());

        // Check get/set
        cdt.setColor(anyColor());
        assertLiteralEquals(ANY_COLOR, cdt.getColor());
    }

    /**
     * Test of {@code Opacity} property.
     */
    @Test
    public void testOpacity() {
        Fill cdt = new Fill();

        // Check default
        assertLiteralEquals(1.0, cdt.getOpacity());

        // Check get/set
        cdt.setOpacity(FF.literal(0.75));
        assertLiteralEquals(0.75, cdt.getOpacity());
    }

    /**
     * Test of the construct setting {@code Color} and {@code Opacity} together.
     */
    @Test
    public void testColorAndOpacity() {
        Fill cdt = new Fill(new Color(255, 255, 0, 128));
        assertLiteralEquals(Color.YELLOW, cdt.getColor());
        assertLiteralEquals(0.5, cdt.getOpacity());
    }
}
