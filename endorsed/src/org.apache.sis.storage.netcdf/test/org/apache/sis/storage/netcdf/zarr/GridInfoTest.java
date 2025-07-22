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
package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.netcdf.base.Axis;
import org.apache.sis.storage.netcdf.base.Decoder;
import org.apache.sis.storage.netcdf.base.Grid;
import org.apache.sis.storage.netcdf.base.GridTest;
import org.apache.sis.util.ArraysExt;
import org.junit.jupiter.api.Test;
import org.opengis.test.dataset.TestData;

import java.io.IOException;

import static org.apache.sis.storage.netcdf.zarr.ZarrDecoderTest.createZarrDecoder;
import static org.apache.sis.storage.netcdf.zarr.ZarrDecoderTest.getPath;
import static org.apache.sis.test.TestUtilities.getSingleton;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests the {@link GridInfo} implementation.
 * passed.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public final class GridInfoTest {
    /**
     * Creates a new test.
     */
    public GridInfoTest() {}

    /**
     * Creates a new decoder for the specified dataset.
     *
     * @return the decoder for the specified dataset.
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    protected Decoder createDecoder(final TestData file) throws IOException, DataStoreException {
        return createZarrDecoder(file.file().toPath());
    }

    /**
     * Tests {@link Grid#getSourceDimensions()} and {@code Grid.getTargetDimensions()}.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testDimensions() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));

        Grid geometry = getSingleton(filter(decoder.getGridCandidates()));
        assertEquals(2, geometry.getSourceDimensions());
        assertEquals(2, geometry.getAxes(decoder).length);

        final Decoder geoDecoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/geo.zarr"));

        geometry = getSingleton(filter(geoDecoder.getGridCandidates()));
        assertEquals(3, geometry.getSourceDimensions());
        assertEquals(3, geometry.getAxes(decoder).length);
    }

    /**
     * Returns the coordinate system axes for the <abbr>CRS</abbr> decoded from the given file.
     */
    private Axis[] axes(final String path) throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath(path));

        return getSingleton(filter(decoder.getGridCandidates())).getAxes(decoder);
    }


    /**
     * Tests {@link Grid#getAxes(Decoder)} on a two-dimensional dataset.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testAxes2D() throws IOException, DataStoreException {
        final Axis[] axes = axes("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr");
        assertEquals(2, axes.length);
        final Axis x = axes[0];
        final Axis y = axes[1];

        assertEquals('t', y.abbreviation);
        assertEquals(5, y.getMainSize().getAsLong());
        assertEquals(60, x.getMainSize().getAsLong());

        final Axis[] geoAxes = axes("/org/apache/sis/storage/netcdf/resources/zarr/geo.zarr");
        assertEquals(3, geoAxes.length);
        final Axis time = geoAxes[2];
        final Axis lat = geoAxes[1];
        final Axis lon = geoAxes[0];

        assertEquals('t', time.abbreviation);
        assertEquals('φ', lat.abbreviation);
        assertEquals('λ', lon.abbreviation);
        assertEquals(5, time.getMainSize().getAsLong());
        assertEquals(6, lat.getMainSize().getAsLong());
        assertEquals(10, lon.getMainSize().getAsLong());
    }

    /**
     * Filters out the one-dimensional coordinate systems created by {@code GridGeometry}
     * but not by the UCAR library.
     *
     * @return the filtered grid geometries to test.
     */
    protected Grid[] filter(final Grid[] geometries) {
        final Grid[] copy = new Grid[geometries.length];
        int count = 0;
        for (final Grid geometry : geometries) {
            if (geometry.getSourceDimensions() != 1) {
                copy[count++] = geometry;
            }
        }
        return ArraysExt.resize(copy, count);
    }
}
