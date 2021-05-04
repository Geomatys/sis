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
package org.apache.sis.storage.geotiff;

import java.util.List;
import java.io.IOException;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridCoverage;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.SampleDimension;
import org.apache.sis.math.Vector;
import org.apache.sis.util.ArgumentChecks;


/**
 * Raster data obtained from a GeoTIFF file in the domain requested by user. The number of dimensions is 2
 * for standard TIFF files, but this class accepts higher number of dimensions if 3- or 4-dimensional data
 * are stored in a GeoTIFF file using some convention.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class DataSubset extends GridCoverage {
    /**
     * The GeoTIFF reader which contain this {@code DataSubset}.
     * Used for fetching information like the input channel and where to report warnings.
     */
    private final Reader reader;

    /**
     * For each tile, the byte offset of that tile as compressed and stored on disk.
     * Tile X index varies fastest, followed by tile Y index, then tile Z index if any.
     *
     * @see ImageFileDirectory#tileOffsets
     * @see #tileStrides
     */
    private final Vector tileOffsets;

    /**
     * For each tile, the number of (compressed) bytes in that tile.
     * Elements are in the same order than {@link #tileOffsets}.
     *
     * @see ImageFileDirectory#tileByteCounts
     * @see #tileStrides
     */
    private final Vector tileByteCounts;

    /**
     * Number of tiles along each dimension, inside the domain specified by the user.
     * The array length is 2 for a standard TIFF file, but this class accepts a higher number of
     * dimensions if 3- or 4-dimensional data are stored in a GeoTIFF file using some convention.
     * This is the number of tiles in the requested domain, not necessarily in the whole image on file.
     * The first tile is located at indices  (0, 0, …).
     */
    private final int[] numTiles;

    /**
     * Values by which to multiply each tile coordinates for obtaining the index in
     * {@link #tileOffsets} and {@link #tileByteCounts} vectors. The length of this
     * array is same as {@link #numTiles}.
     */
    private final int[] tileStrides;

    /**
     * The method for transferring pixel values from GeoTIFF file to this {@code Raster}.
     * There is different subtypes depending on decompression methods.
     */
    private final PixelDecoder decoder;

    /**
     * Creates a new data subset. All parameters should have been validated
     * by {@link ImageFileDirectory#validateMandatoryTags()} before this call.
     *
     * @param  parent          the resource which is creating this {@code DataSubset}.
     * @param  domain          the sub-region extent, CRS and conversion from cell indices to CRS.
     * @param  ranges          sample dimensions for each image band.
     * @param  tileOffsets     the byte offset of each tile relative to the beginning of the TIFF file.
     * @param  tileByteCounts  number of byte for each tile stored in the TIFF file.
     * @param  numTiles        number of tiles along each dimension, inside the domain specified by the user.
     * @param  tileStrides     values by which to multiply each tile coordinates for obtaining index in vectors.
     * @throws ArithmeticException if the number of tiles overflows 32 bits integer arithmetic.
     */
    DataSubset(final DataCube parent, final GridGeometry domain, final List<? extends SampleDimension> ranges,
               final Vector tileOffsets, final Vector tileByteCounts, final long[] numTiles, final int[] tileStrides,
               final PixelDecoder decoder)
    {
        super(domain, ranges);
        this.reader         = parent.reader;
        this.tileOffsets    = tileOffsets;
        this.tileByteCounts = tileByteCounts;
        this.tileStrides    = tileStrides;
        this.decoder        = decoder;
        this.numTiles       = new int[numTiles.length];
        for (int i=0; i<numTiles.length; i++) {
            this.numTiles[i] = Math.toIntExact(numTiles[i]);
        }
    }

    /**
     * Reads the tile at the given tile coordinates.
     * This method is thread-safe.
     *
     * @param  tile  indices of the tile to read.
     * @return image decoded from the GeoTIFF file.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreContentException if the sample model is not supported.
     * @throws RuntimeException if the Java2D image can not be created for another reason
     *         (too many exception types to list them all).
     */
    private WritableRaster readTile(final int[] tile) throws IOException, DataStoreContentException {
        synchronized (reader.store) {
            int index = 0;
            for (int i=0; i<tileStrides.length; i++) {
                final int t = tile[i];
                ArgumentChecks.ensureBetween("tile", 0, numTiles[i] - 1, t);
                index = Math.addExact(index, Math.multiplyExact(tileStrides[i], t));
            }
            final long offset = Math.addExact(reader.origin, tileOffsets.longValue(index));
            return decoder.readSlice(reader.input, offset);
        }
    }

    @Override
    public RenderedImage render(final GridExtent sliceExtent) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
