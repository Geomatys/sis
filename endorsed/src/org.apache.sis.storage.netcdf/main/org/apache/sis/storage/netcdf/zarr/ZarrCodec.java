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

/**
 * Enumeration of Zarr codecs used for data compression and encoding in Zarr datasets.
 *
 * @author  Quentin Bialota (Geomatys)
 */
enum ZarrCodec {
    BLOSC (Type.BYTES_TO_BYTES, BloscCodec.class),
    BYTES (Type.ARRAY_TO_BYTES, BytesCodec.class),
    CRC32C (Type.BYTES_TO_BYTES, null), // Not supported
    GZIP (Type.BYTES_TO_BYTES, null), // Not supported
    SHARDING_INDEXED (Type.ARRAY_TO_BYTES, null), // Not supported
    TRANSPOSE (Type.ARRAY_TO_ARRAY, null), // Not supported
    ZSTD (Type.BYTES_TO_BYTES, ZstdCodec.class),
    VLEN_UTF8 (Type.ARRAY_TO_BYTES, VlenUtf8Codec.class);

    /**
     * The type of Zarr codec, indicating how it transforms data.
     */
    public enum Type {
        ARRAY_TO_ARRAY,
        ARRAY_TO_BYTES,
        BYTES_TO_BYTES;
    }

    /**
     * The type of Zarr codec, indicating how it transforms data.
     */
    public final Type zarrCodecType;

    /**
     * The class implementing the codec functionality, if applicable.
     * This is null for codecs that do not have a specific implementation class.
     */
    public final Class<?> codecClass;

    /**
     * Creates a new enumeration value.
     */
    private ZarrCodec(final Type zarrCodecType, final Class<?> codecClass) {
        this.zarrCodecType = zarrCodecType;
        this.codecClass = codecClass;
    }

    /**
     * Returns the Zarr codec type.
     * @return the type of Zarr codec
     */
    public Type zarrCodecType() {
        return zarrCodecType;
    }

    /**
     * Returns the class implementing the codec functionality.
     * @return the codec class, or null if not applicable
     */
    public Class<?> codecClass() {
        return codecClass;
    }
}