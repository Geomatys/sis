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
package org.apache.sis.storage.netcdf.base;

import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.image.internal.shared.RasterFactory;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.base.TiledGridCoverage;
import org.apache.sis.storage.base.TiledGridResource;
import org.opengis.util.GenericName;

import java.awt.Point;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Data loaded from a {@link TiledRasterResource} and potentially shown as {@link RenderedImage}.
 * The rendered image is usually mono-banded, but may be multi-banded in some special cases
 * handled by {@link TiledRasterResource#read(GridGeometry, int...)}.
 *
 * @author  Quentin Bialota
 */
final class TiledRaster extends TiledGridCoverage {

    private final Variable[] bands;
    private final DataType dataType;
    private final TiledRasterResource resource;

    /**
     * Creates a new raster from the given resource.
     * @param  resource    the resource from which to read tiles.
     * @param  subset      the subset of grid data to load.
     */
    TiledRaster(TiledRasterResource resource, TiledGridResource.Subset subset)
    {
        super(subset);
        this.resource = resource;

        // subset.includedBands contains the indices of bands requested by the user.
        // We filter the resource bands to only keep those needed.
        Variable[] allBands = resource.getBands();
        if (includedBands != null) {
            this.bands = new Variable[includedBands.length];
            for(int i=0; i<includedBands.length; i++) {
                this.bands[i] = allBands[includedBands[i]];
            }
        } else {
            this.bands = allBands;
        }
        this.dataType = bands[0].getDataType();
    }

    /**
     * Returns an human-readable identification of this coverage.
     * The namespace should be the {@linkplain #filename() filename}
     * and the tip can be an image index, citation, or overview level.
     */
    @Override
    protected final GenericName getIdentifier() {
        return resource.getIdentifier().orElse(null);
    }

    /**
     * Reads tiles from the resource using the given iterator.
     *
     * @param iterator  an iterator over the tiles that intersect the Area Of Interest specified by user.
     * @return the tiles read from the resource.
     * @throws IOException if an I/O error occurred while reading tiles.
     * @throws DataStoreException if a data store error occurred while reading tiles.
     */
    @Override
    protected Raster[] readTiles(TileIterator iterator) throws IOException, DataStoreException {
        Raster[] results = new Raster[iterator.tileCountInQuery];
        List<Tile> missings = new ArrayList<>();

        // Identify what is cached and what is missing
        do {
            Raster tile = iterator.getCachedTile();
            if (tile != null) {
                results[iterator.getTileIndexInResultArray()] = tile;
            } else {
                /*
                 * Tile not yet loaded. Add to a queue of tiles to load later.
                 * We create a TileSnapshot now to capture the iterator state
                 */
                missings.add(new Tile(iterator));
            }
        } while (iterator.next());

        // Read missing tiles
        // Assume all bands have the same chunk shape
        int[] chunkShape = bands[0].getTileShape();
        for (Tile tile : missings) {

            // Reconstruct Zarr chunk indices from the snapshot
            int[] chunkIndices = new int[chunkShape.length];
            for(int i=0; i<chunkIndices.length; i++) {
                chunkIndices[i] = Math.toIntExact(tile.resourceCoordinates[i]);
            }
            // Invert row/column for ordinal to Zarr mapping
            chunkIndices = swapIndices(chunkIndices);

            // Read Data for all bands
            Buffer[] bankBuffers = new Buffer[bands.length];
            for (int i = 0; i < bands.length; i++) {
                Object primitiveArray = bands[i].readTile(chunkIndices);
                bankBuffers[i] = wrapBuffer(primitiveArray, dataType);
            }

            // Create Raster
            DataBuffer dataBuffer = RasterFactory.wrap(dataType.rasterDataType, bankBuffers);

            // The TileSnapshot captured the origin X/Y relative to the RenderedImage
            Point origin = new Point(tile.originX, tile.originY);

            WritableRaster raster = WritableRaster.createWritableRaster(this.model, dataBuffer, origin);

            Raster cached = tile.cache(raster);
            results[tile.getTileIndexInResultArray()] = cached;
        }

        return results;
    }

    /**
     * Swap the first two indices in the given array.
     * @param indices the indices to swap.
     * @return the swapped indices.
     */
    private int[] swapIndices(int[] indices) {
        int temp = indices[0];
        indices[0] = indices[1];
        indices[1] = temp;
        return indices;
    }

    /**
     * Helper to wrap the primitive array returned by Zarr decoders into the NIO Buffer required by RasterFactory.
     */
    private Buffer wrapBuffer(Object array, DataType type) {
        switch (dataType.rasterDataType) {
            case FLOAT:
                return FloatBuffer.wrap((float[]) array);
            case DOUBLE:
                return DoubleBuffer.wrap((double[]) array);
            case BYTE:
                return ByteBuffer.wrap((byte[]) array);
            case SHORT:
            case USHORT:
                return ShortBuffer.wrap((short[]) array);
            case INT:
                return IntBuffer.wrap((int[]) array);
            default:
                throw new IllegalArgumentException("Unsupported data type for wrapping: " + type);
        }
    }

    /**
     * Information about a tile to be read. A list of {@code Tile} is created and sorted by increasing offsets
     * before the read operation begins, in order to read tiles in the order they are written in the TIFF file.
     */
    private static final class Tile extends Snapshot {

        final long[] resourceCoordinates;

        Tile(final AOI domain) {
            super(domain);
            this.resourceCoordinates = domain.getTileCoordinatesInResource();
        }
    }
}
