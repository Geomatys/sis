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
package org.apache.sis.coverage.privy;

import java.awt.Dimension;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestCase;


/**
 * Tests {@link ImageLayoutTest}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class ImageLayoutTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public ImageLayoutTest() {
    }

    /**
     * Verifies that {@link ImageLayout#SUGGESTED_TILE_CACHE_SIZE} is strictly positive.
     */
    @Test
    public void verifySuggestedTileCacheSize() {
        assertTrue(ImageLayout.SUGGESTED_TILE_CACHE_SIZE >= 1);
    }

    /**
     * Tests {@link ImageLayout#suggestTileSize(int, int, boolean)}.
     */
    @Test
    public void testSuggestTileSize() {
        final Dimension size = ImageLayout.DEFAULT.suggestTileSize(367877, 5776326, true);
        assertEquals(511, size.width);
        assertEquals(246, size.height);
    }
}
