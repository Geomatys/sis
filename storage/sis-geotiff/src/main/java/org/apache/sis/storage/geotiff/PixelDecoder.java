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

import java.nio.Buffer;
import java.io.IOException;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.BandedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.SinglePixelPackedSampleModel;
import java.awt.image.WritableRaster;
import org.apache.sis.image.DataType;
import org.apache.sis.internal.coverage.j2d.ImageUtilities;
import org.apache.sis.internal.coverage.j2d.RasterFactory;
import org.apache.sis.internal.storage.io.Region;
import org.apache.sis.internal.storage.io.ChannelDataInput;
import org.apache.sis.internal.storage.io.HyperRectangleReader;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.util.ArraysExt;


/**
 * Method for transferring pixel values from GeoTIFF tile to a {@link Raster}.
 * The base class transfers uncompressed data. Compressed data are handled by
 * specialized subclasses.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
class PixelDecoder {
    /**
     * Size of all tiles in the GeoTIFF image (without clipping and subsampling).
     */
    private final int[] tileSize;

    /**
     * Subsampling along each dimension. All values shall be greater than zero.
     */
    private final int[] subsamplings;

    /**
     * The sample model for all rasters. The size of this sample model is the values of
     * the two first elements of {@link #tileSize} divided by subsampling after clipping.
     */
    protected final SampleModel model;

    /**
     * Creates a new pixel reader for the given type.
     *
     * @param  tileSize      size of all tiles before clipping and subsampling.
     * @param  subsamplings  subsampling along each dimension. All values shall be greater than zero.
     * @param  numBands      the number of bands in the raster to read.
     * @param  layout        the Java2D type to use in destination image and other information.
     * @throws IllegalArgumentException if a {@link SampleModel} can not be created from the given parameters.
     */
    PixelDecoder(final int[] tileSize, final int[] subsamplings, final int numBands, final DataCube.Layout layout) {
        this.subsamplings = subsamplings;
        this.tileSize     = tileSize;
        final int width   = tileSize[0] / subsamplings[0];
        final int height  = tileSize[1] / subsamplings[1];
        final int type    = layout.type.ordinal();
        if (layout.bitsPerSample < DataBuffer.getDataTypeSize(type)) {
            /*
             * Supported types for both sample models are TYPE_BYTE, TYPE_USHORT, and TYPE_INT.
             * Note that if the TIFF data are signed bytes, we have TYPE_SHORT which will cause
             * an exception below.
             */
            if (numBands == 1) {
                model = new MultiPixelPackedSampleModel(type, width, height, layout.bitsPerSample);
            } else if (!layout.isPlanar) {
                final int[] bitMasks = new int[numBands];
                model = new SinglePixelPackedSampleModel(type, width, height, bitMasks);
            } else {
                // TODO: we can support that with a little bit more work.
                throw new IllegalArgumentException("Unsupported data format.");
            }
        } else if (layout.isPlanar) {
            tileSize[2] = numBands;
            model = new BandedSampleModel(type, width, height, numBands);
        } else {
            model = new PixelInterleavedSampleModel(type, width, height, numBands,
                    Math.multiplyExact(numBands, width), ArraysExt.range(0, numBands));
        }
    }

    /**
     * Reads a two-dimensional slice of the data cube. Implementation in base class assumes uncompressed data.
     * Subclasses must override for handling decompression.
     *
     * @param  input   the channel to read.
     * @param  offset  position in the channel where tile data begins.
     * @return image decoded from the GeoTIFF file.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreContentException if the sample model is not supported.
     * @throws RuntimeException if the Java2D image can not be created for another reason
     *         (too many exception types to list them all).
     */
    WritableRaster readSlice(final ChannelDataInput input, final long offset)
            throws IOException, DataStoreContentException
    {
        final HyperRectangleReader reader = new HyperRectangleReader(
                ImageUtilities.toNumberEnum(model.getDataType()), input, offset);
        /*
         * "Banks" (in `java.awt.image.DataBuffer` sense) are synonymous to "bands" for planar image only.
         * Otherwise there is only one bank not matter the amount of bands. Each bank is read separately.
         */
        final Buffer[] banks = new Buffer[model instanceof BandedSampleModel ? model.getNumBands() : 1];
        final long[] size        = new long[] {tileSize[0], tileSize[1], banks.length};
        final int[]  subsample2D = new int[]  {subsamplings[0], subsamplings[1], 1};
        final long[] regionLower = new long[3];     // TODO: allow subregion if only one tile along x or y.
        final long[] regionUpper = new long[] {regionLower[0] + tileSize[0],
                                               regionLower[1] + tileSize[1], 0};
        for (int b=0; b < banks.length; b++) {
            regionLower[2] = b;
            regionUpper[2] = b + 1;
            banks[b] = reader.readAsBuffer(new Region(size, regionLower, regionUpper, subsample2D));
        }
        final DataBuffer buffer = RasterFactory.wrap(DataType.forDataBufferType(model.getDataType()), banks);
        return WritableRaster.createWritableRaster(model, buffer, null);
    }
}
