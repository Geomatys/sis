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
import org.apache.sis.storage.netcdf.base.DataType;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Abstract base class for Zarr codecs, providing common functionality
 *
 * @author  Quentin Bialota (Geomatys)
 */
abstract class AbstractZarrCodec {

    /**
     * The Zarr codec type.
     */
    protected final ZarrCodec codec;

    /**
     * Configuration parameters for the codec.
     * This can include compression levels, encoding options, etc.
     */
    protected final Map<String, Object> configuration;

    /**
     * Constructor for AbstractZarrCodec.
     *
     * @param codec the Zarr codec type
     * @param configuration the configuration parameters for the codec
     */
    protected AbstractZarrCodec(ZarrCodec codec, Map<String, Object> configuration) {
        this.codec = codec;
        this.configuration = configuration;
    }

    /**
     * Returns the Zarr codec type.
     * @return the Zarr codec type
     */
    public ZarrCodec codec() {
        return codec;
    }

    /**
     * Returns a specific configuration parameter by key.
     * @return the configuration parameter value, or null if not found
     */
    public Object getConfiguration(String key) {
        if (configuration != null) {
            return configuration.get(key);
        }
        return null;
    }

    /**
     * Computes the output representation type, given an input ("decoded") representation type.
     * @param decodedType Array info: shape, data type, etc. (you may want a struct for this!)
     * @return The output type after encoding (e.g. different dtype, shape, etc.; or byte[] for array->bytes)
     */
    abstract ZarrRepresentationType computeEncodedType(ZarrRepresentationType decodedType);

    /**
     * Encodes given decoded value (array or other), producing encoded value (array or bytes)
     */
    abstract Object encode(Object decodedValue, ZarrRepresentationType decodedType) throws DataStoreException;

    /**
     * Decodes encoded value (byte[] for array->bytes, any for bytes->bytes), returning decoded value.
     */
    abstract Object decode(Object encodedValue, ZarrRepresentationType decodedType) throws DataStoreException;

    /**
     * Decodes elements from a ByteBuffer directly into a typed output array.
     * Only the requested elements are read — no intermediate typed array is allocated.
     * Supports subsampling via the {@code step} parameter.
     *
     * @param src    the source ByteBuffer containing the raw bytes (with correct byte order)
     * @param srcPos the starting position in element units (not bytes) within the source
     * @param dst    the destination typed array (e.g., float[], int[], etc.)
     * @param dstPos the starting position in the destination array
     * @param count  the number of source elements to consider (before subsampling)
     * @param step   the subsampling step (1 = contiguous copy, >1 = skip elements)
     * @param type   the data type of the elements
     * @throws DataStoreException if the data type is unknown or unsupported
     */
    void decodeRegion(ByteBuffer src, int srcPos, Object dst, int dstPos, int count, int step, DataType type)
            throws DataStoreException {
            throw new UnsupportedOperationException("decodeRegion is not implemented for codec: " + codec +
                    " . This method needs to be implemented by array<->bytes codecs.");
    }
}
