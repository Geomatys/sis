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
import java.util.Arrays;
import java.nio.file.Path;
import org.apache.sis.image.DataType;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.coverage.SampleDimension;
import org.apache.sis.coverage.grid.GridCoverage;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridDerivation;
import org.apache.sis.coverage.grid.GridRoundingMode;
import org.apache.sis.internal.storage.AbstractGridResource;
import org.apache.sis.internal.storage.ResourceOnFileSystem;
import org.apache.sis.internal.util.Numerics;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.math.Vector;


/**
 * One or many GeoTIFF images packaged as a single resource.
 * This is typically a single two-dimensional image represented as a {@link ImageFileDirectory}.
 * But it can also be a stack of images organized in a <var>n</var>-dimensional data cube,
 * or a pyramid of images with their overviews used when low resolution images is requested.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
abstract class DataCube extends AbstractGridResource implements ResourceOnFileSystem {
    /**
     * The GeoTIFF reader which contain this {@code DataCube}.
     * Used for fetching information like the input channel and where to report warnings.
     */
    final Reader reader;

    /**
     * Creates a new data cube.
     *
     * @param reader  information about the input stream to read, the metadata and the character encoding.
     */
    DataCube(final Reader reader) {
        super(reader.store.listeners());
        this.reader = reader;
    }

    /**
     * Shortcut for a frequently requested information.
     */
    final String filename() {
        return reader.input.filename;
    }

    /**
     * Information about pixel layout (data type, number of pixels, …) in the TIFF file.
     */
    static final class Layout {
        /** The type to use for storing raster data. */
        final DataType type;

        /** Number of bits per component. */
        final int bitsPerSample;

        /** Whether the components are stored in separate “component planes”. */
        final boolean isPlanar;

        /** Creates new layout information. */
        Layout(final DataType type, final int bitsPerSample, final boolean isPlanar) {
            this.type          = type;
            this.bitsPerSample = bitsPerSample;
            this.isPlanar      = isPlanar;
        }
    }

    /**
     * Returns the type to use for storing raster data, together with the number of bits per sample.
     * The type should be the one used in the GeoTIFF file as much as possible, but may sometime be wider.
     *
     * @throws DataStoreContentException if the type is not recognized.
     */
    protected abstract Layout getLayout() throws DataStoreContentException;

    /**
     * Returns the size in pixels of all tiles in this data cube. The array length shall be equal to
     * {@link GridGeometry#getDimension()}, unless the grid geometry has one extra dimension for the
     * vertical axis.
     *
     * @see #getGridGeometry()
     */
    protected abstract int[] getTileSize();

    /**
     * Gets the stream position or the length in bytes of compressed tile arrays in the GeoTIFF file.
     * Values in the returned vector are {@code long} primitive type.
     *
     * @param  length  {@code false} for {@code tileOffsets} or {@code true} for {@code tileByteCounts}.
     * @return stream position (relative to file beginning) or length of compressed tile arrays, in bytes.
     */
    abstract Vector getTileArrayInfo(boolean length);

    /**
     * Loads a subset of the grid coverage represented by this resource.
     *
     * @param  domain  desired grid extent and resolution, or {@code null} for reading the whole domain.
     * @param  range   0-based index of sample dimensions to read, or an empty sequence for reading all ranges.
     * @return the grid coverage for the specified domain and range.
     * @throws DataStoreException if an error occurred while reading the grid coverage data.
     */
    @Override
    public final GridCoverage read(final GridGeometry domain, final int... range) throws DataStoreException {
        final long startTime = System.nanoTime();
        final GridCoverage coverage;
        try {
            synchronized (reader.store) {
                coverage = subset(domain, range);
            }
        } catch (RuntimeException e) {
            throw new DataStoreException(reader.errors().getString(Errors.Keys.CanNotRead_1, filename()), e);
        }
        logReadOperation(reader.store.path, coverage.getGridGeometry(), startTime);
        return coverage;
    }

    /**
     * Creates a {@link GridCoverage} which will load pixel data in the given domain,
     * but without loading data immediately. This method does not perform I/O operations.
     *
     * @throws ArithmeticException if pixel indices exceed 64 bits integer capacity.
     */
    private DataSubset subset(GridGeometry domain, final int... range) throws DataStoreException {
        final List<SampleDimension> bands = getSampleDimensions();
        final RangeArgument  rangeIndices = validateRangeArgument(bands.size(), range);
        final GridGeometry   gridGeometry = getGridGeometry();
        final GridExtent     fullSize     = gridGeometry.getExtent();
        final int[]          tileSize     = getTileSize();
        final int            dimension    = tileSize.length;            // May be less than `fullSize.getDimension()`.
        final int[]          subsamplings;
        GridExtent           areaOfInterest;
        if (domain == null) {
            domain         = gridGeometry;
            areaOfInterest = fullSize;
            subsamplings   = new int[gridGeometry.getDimension()];
            Arrays.fill(subsamplings, 1);
        } else {
            final GridDerivation target = gridGeometry.derive()
                    .rounding(GridRoundingMode.ENCLOSING).subgrid(domain);
            areaOfInterest = target.getIntersection();
            subsamplings   = target.getSubsamplings();
            domain         = target.build();
        }
        Vector tileOffsets    = getTileArrayInfo(false);
        Vector tileByteCounts = getTileArrayInfo(true);
        /*
         * Compute an initial number of tiles from the image size and tile size.
         * Those numbers will be modified later if a sub-domain has been requested.
         */
        final long[] numTiles = new long[dimension];
        for (int i=0; i<dimension; i++) {
            numTiles[i] = Numerics.ceilDiv(fullSize.getSize(i), tileSize[i]);
        }
        /*
         * Compute the values by which to multiply each tile coordinate for obtaining
         * the index in `tileOffsets` and `tileByteCounts` vectors. This computation
         * must be done before to clip `numTiles` to user-specified domain.
         */
        final int[] tileStrides = new int[dimension];
        tileStrides[0] = 1;
        for (int i=1; i<dimension; i++) {
            tileStrides[i] = Math.toIntExact(Math.multiplyExact(numTiles[i], tileStrides[i-1]));
        }
        /*
         * If a sub-domain is requested, modify the tile indices in order to include only
         * the tiles that intersect the specified domain. Since we will still use (0,0,…)
         * as the indices of the first tile, we need to have that tile at index 0 in the
         * `tileOffsets` and `tileByteCounts` vectors.
         */
        if (!fullSize.equals(areaOfInterest)) {
            long firstTileIndex = 0;
            final long[] lowerDiff = new long[dimension];
            final long[] upperDiff = new long[dimension];
            for (int i=0; i<dimension; i++) {
                final long low       = areaOfInterest.getLow(i);                            // Always ≥ 0.
                final long high      = areaOfInterest.getHigh(i);                           // Inclusive
                final int  size      = tileSize[i];
                final long tileLower = low / size;                                          // Always ≥ 0.
                final long tileUpper = Math.incrementExact(high / size);                    // Exclusive.
                upperDiff[i]         = Math.multiplyExact(tileUpper, size) - high - 1;      // Always ≥ 0.
                lowerDiff[i]         = low % size;                                          // low - tileLower*size
                numTiles[i]          = tileUpper - tileLower;
                firstTileIndex      += tileLower * tileStrides[i];
            }
            final int start = Math.toIntExact(firstTileIndex);
            tileOffsets     = tileOffsets   .subList(start, tileOffsets.size());
            tileByteCounts  = tileByteCounts.subList(start, tileByteCounts.size());
            areaOfInterest  = areaOfInterest.expand(lowerDiff, upperDiff);                  // TODO: need to clip.
            /*
             * TODO: need to recompute `domain`. We should put above calculation directly in `GridDerivation`.
             * See `GridDerivation.margin`; we may be able to apply tiling in the same code than margins.
             */
        }
        /*
         * TODO: take selected bands in account.
         */
        final PixelDecoder decoder = new PixelDecoder(tileSize, subsamplings, bands.size(), getLayout());
        return new DataSubset(this, domain, bands, tileOffsets, tileByteCounts, numTiles, tileStrides, decoder);
    }

    /**
     * Gets the paths to files used by this resource, or an empty array if unknown.
     */
    @Override
    public Path[] getComponentFiles() {
        final Path location = reader.store.path;
        return (location != null) ? new Path[] {location} : new Path[0];
    }
}
