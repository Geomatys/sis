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
package org.apache.sis.storage.aggregate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.awt.image.DataBufferInt;
import java.awt.image.RenderedImage;
import org.opengis.util.LocalName;
import org.opengis.referencing.datum.PixelInCell;
import org.apache.sis.util.iso.Names;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.coverage.SampleDimension;
import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.GridCoverage;
import org.apache.sis.coverage.grid.BufferedGridCoverage;
import org.apache.sis.internal.storage.MemoryGridResource;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.GridCoverageResource;
import org.apache.sis.referencing.crs.HardCodedCRS;
import org.apache.sis.test.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests {@link BandAggregateGridResource}.
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.4
 * @since   1.4
 */
public final class BandAggregateGridResourceTest extends TestCase {
    /**
     * Width and height of images created for tests.
     */
    private static final int WIDTH = 2, HEIGHT = 3;

    /**
     * The grid geometry of all coverages created in this test.
     */
    private final GridGeometry domain;

    /**
     * Creates a new test case.
     */
    public BandAggregateGridResourceTest() {
        domain = new GridGeometry(new GridExtent(WIDTH, HEIGHT), PixelInCell.CELL_CENTER,
                                  MathTransforms.identity(2), HardCodedCRS.WGS84);
    }

    /**
     * Tests aggregation of two resources having one band each.
     * All source coverages share the same grid geometry.
     *
     * @throws DataStoreException if an error occurred while reading a resource.
     */
    @Test
    public void aggregateBandsFromSingleBandSources() throws DataStoreException {
        final GridCoverageResource first  = singleValuePerBand(17);
        final GridCoverageResource second = singleValuePerBand(23);
        final var aggregation = new BandAggregateGridResource(first, second);

        assertAllPixelsEqual(aggregation.read(null), 17, 23);
        assertAllPixelsEqual(aggregation.read(null, 0), 17);
        assertAllPixelsEqual(aggregation.read(null, 1), 23);
    }

    /**
     * Tests aggregation of three resources having many bands.
     * All source coverages share the same grid geometry.
     *
     * @throws DataStoreException if an error occurred while reading a resource.
     */
    @Test
    public void aggregateBandsFromMultiBandSources() throws DataStoreException {
        final GridCoverageResource firstAndSecondBands = singleValuePerBand(101, 102);
        final GridCoverageResource thirdAndFourthBands = singleValuePerBand(103, 104);
        final GridCoverageResource fifthAndSixthBands  = singleValuePerBand(105, 106);

        var aggregation = new BandAggregateGridResource(firstAndSecondBands, thirdAndFourthBands, fifthAndSixthBands);
        aggregation.getIdentifier().ifPresent(name -> fail("No name provided at creation, but one was returned: " + name));
        assertAllPixelsEqual(aggregation.read(null), 101, 102, 103, 104, 105, 106);
        assertAllPixelsEqual(aggregation.read(null, 1, 2, 4, 5), 102, 103, 105, 106);
        assertAllPixelsEqual(aggregation.read(null, 3, 4), 104, 105);
        /*
         * Test again, but specifying a subset of bands to the constructor.
         * In addition, band order in one of the 3 coverages is modified.
         */
        final LocalName testName = Names.createLocalName(null, null, "test-name");
        aggregation = new BandAggregateGridResource(null, testName,
                new GridCoverageResource[] {firstAndSecondBands, thirdAndFourthBands, fifthAndSixthBands},
                new int[][] {null, new int[] {1, 0}, new int[] {1}}, null);

        assertEquals(testName, aggregation.getIdentifier().orElse(null));
        assertAllPixelsEqual(aggregation.read(null), 101, 102, 104, 103, 106);
        assertAllPixelsEqual(aggregation.read(null, 2, 4), 104, 106);
    }

    /**
     * Creates a new grid coverage resource with bands having the given values.
     * The length of the {@code bandValues} array is the number of bands to create.
     * In a given band <var>b</var>, all pixels have the {@code bandValues[b]}.
     *
     * @param  bandValues  sample values for all pixels.
     * @return a coverage with an image where all pixels have the specified sample values.
     */
    private GridCoverageResource singleValuePerBand(final int... bandValues) {
        final var builder = new SampleDimension.Builder();
        final List<SampleDimension> samples = IntStream.of(bandValues)
                .mapToObj(b -> builder.setName("Band with value " + b).build())
                .collect(Collectors.toList());

        final int numBands = bandValues.length;
        final int[] data = new int[WIDTH * HEIGHT * numBands];
        for (int i=0; i < data.length; i += numBands) {
            System.arraycopy(bandValues, 0, data, i, numBands);
        }
        final var values = new DataBufferInt(data, data.length);
        return new MemoryGridResource(null, new BufferedGridCoverage(domain, samples, values));
    }

    /**
     * Asserts that all pixels in the rendered image have the expected value.
     *
     * @param  coverage  the coverage for which to verify pixel values.
     * @param  pixel     the expected pixel value.
     */
    private static void assertAllPixelsEqual(final GridCoverage coverage, final int... pixel) {
        final int numBands = pixel.length;
        final RenderedImage rendering = coverage.render(null);
        assertEquals("minX",     0,        rendering.getMinX());
        assertEquals("minY",     0,        rendering.getMinY());
        assertEquals("width",    WIDTH,    rendering.getWidth());
        assertEquals("height",   HEIGHT,   rendering.getHeight());
        assertEquals("numBands", numBands, rendering.getSampleModel().getNumBands());
        final int[] expected = new int[WIDTH * HEIGHT * numBands];
        for (int i=0; i < expected.length; i += numBands) {
            System.arraycopy(pixel, 0, expected, i, numBands);
        }
        assertArrayEquals(expected, rendering.getData().getPixels(0, 0, WIDTH, HEIGHT, (int[]) null));
    }
}