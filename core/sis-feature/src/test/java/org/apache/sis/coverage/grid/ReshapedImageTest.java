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
package org.apache.sis.coverage.grid;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import org.apache.sis.test.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.apache.sis.test.FeatureAssert.assertValuesEqual;


/**
 * Tests the {@link ReshapedImage} implementation.
 *
 * @author  Johann Sorel (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public final strictfp class ReshapedImageTest extends TestCase {
    /**
     * Tests with a request starting on the left and on top of data.
     */
    @Test
    public void testRequestBefore() {
        final BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_BYTE_GRAY);
        final WritableRaster raster = image.getRaster();
        raster.setSample(0, 0, 0, 1);
        raster.setSample(1, 0, 0, 2);
        raster.setSample(0, 1, 0, 3);
        raster.setSample(1, 1, 0, 4);

        final ReshapedImage trs = new ReshapedImage(image, -1, -2, 4, 4);
        assertEquals(1, trs.getMinX());
        assertEquals(2, trs.getMinY());
        assertValuesEqual(trs.getData(), 0, new int[][] {
            {1, 2},
            {3, 4}
        });
    }
}