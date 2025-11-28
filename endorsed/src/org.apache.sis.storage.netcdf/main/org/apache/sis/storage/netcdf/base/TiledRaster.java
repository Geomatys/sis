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
import org.apache.sis.storage.base.TiledGridCoverage;
import org.apache.sis.storage.base.TiledGridResource;
import org.opengis.util.GenericName;

import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
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

    @Override
    protected Raster[] readTiles(TileIterator iterator) throws Exception {
        Raster[] results = new Raster[iterator.tileCountInQuery];
        List<Tile> missings = new ArrayList<>();

        // 1. Identify what is cached and what is missing
        // Use do-while because iterator starts "on" the first tile.
        do {
            Raster tile = iterator.getCachedTile();
            if (tile != null) {
                results[iterator.getTileIndexInResultArray()] = tile;
            } else {
                // Snapshot the current position of the iterator
                missings.add(new Tile(iterator));
            }
        } while (iterator.next());

        // 2. Read missing tiles
        // Note: DataSubset sorts here for disk efficiency. Zarr is random access (HTTP/S3),
        // so strict sorting isn't as critical, but iterating sequentially is fine.

        int[] chunkShape = bands[0].getTileShape();

        for (Tile snapshot : missings) {

            // Reconstruct Zarr chunk indices from the snapshot
            int[] chunkIndices = new int[chunkShape.length];
            for(int i=0; i<chunkIndices.length; i++) {
                chunkIndices[i] = Math.toIntExact(snapshot.resourceCoordinates[i]);
            }

            // Read Data for all bands
            Buffer[] bankBuffers = new Buffer[bands.length];
            for (int i = 0; i < bands.length; i++) {
                Object primitiveArray = bands[i].readTile(chunkIndices);
                bankBuffers[i] = wrapBuffer(primitiveArray, dataType);
            }

            // Create Raster
            DataBuffer dataBuffer = RasterFactory.wrap(dataType.rasterDataType, bankBuffers);

            // The TileSnapshot captured the origin X/Y relative to the RenderedImage
            Point origin = new Point(snapshot.originX, snapshot.originY);

            WritableRaster raster = WritableRaster.createWritableRaster(this.model, dataBuffer, origin);

            // Cache and store in result
            // Note: We use snapshot.cache(...) which delegates to TiledGridCoverage.cacheTile
            // but we need the original iterator's logic for caching which relies on indexInTileVector.
            // Since we don't have the iterator instance here, we call the coverage method directly.

            // However, TiledGridCoverage.AOI.cache(raster) uses indexInTileVector.
            // We stored indexInTileVector in the snapshot.

//            Raster cached = cacheTile(snapshot.indexInTileVector, raster);
            results[snapshot.getTileIndexInResultArray()] = raster;
        }

        return results;
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

    private Raster createRasterFromBanks(Buffer[] banks, int[] chunkShape) {
        // Assume 2D raster logic from the end of the dimensions (Y, X)
        int width = chunkShape[chunkShape.length - 1];
        int height = (chunkShape.length >= 2) ? chunkShape[chunkShape.length - 2] : 1;

        // Wrap the multiple buffers into a single Java2D DataBuffer
        // RasterFactory.wrap usually handles Buffer[] by creating a DataBuffer with multiple banks
        DataBuffer dataBuffer = RasterFactory.wrap(dataType.rasterDataType, banks);

        // Ensure the sample model matches the buffer size/layout
        // The `model` field comes from the parent TiledGridCoverage (calculated in TiledRasterResource)
        // It should be a BandedSampleModel or ComponentSampleModel compatible with multiple banks.
        return WritableRaster.createWritableRaster(this.model, dataBuffer, new Point(0, 0));
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
