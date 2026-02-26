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

import com.scalableminds.bloscjava.Blosc;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Blosc codec for Zarr.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class BloscCodec extends AbstractZarrCodec {

    /**
     * The Zarr codec type for Blosc.
     */
    private static final ZarrCodec CODEC = ZarrCodec.BLOSC;

    /**
     * Compressor name for Blosc.
     */
    private Blosc.Compressor compressor;

    /**
     * Compression level for Blosc.
     */
    private int level;

    /**
     * Use shuffle.
     */
    private Blosc.Shuffle shuffle;

    /**
     * Number of bytes per primitive value;
     */
    private final int typeSize;

    /**
     * Requested size of compressed blocks. (0 for automatic block sizes)
     */
    private int blockSize;

    /**
     * Constructor for ZstdCodec.
     * @param configuration the configuration parameters for the codec, which may include "level" to specify compression level
     */
    public BloscCodec(Map<String, Object> configuration) {
        super(CODEC, configuration);

        Object cname = configuration != null ? configuration.get("cname") : null;
        Object clevel = configuration != null ? configuration.get("clevel") : null;
        Object shuffle = configuration != null ? configuration.get("shuffle") : null;
        Object typesize = configuration != null ? configuration.get("typesize") : null; // Fixed key
        Object blocksize = configuration != null ? configuration.get("blocksize") : null; // Fixed key

        // --- Handle level (Default 1) ---
        if (clevel instanceof Number) {
            this.level = ((Number) clevel).intValue();
        } else if (clevel instanceof String) {
            try {
                this.level = Integer.parseInt((String) clevel);
            } catch (NumberFormatException e) {
                this.level = 1;
            }
        } else {
            this.level = 1;
        }

        // --- Handle typeSize (Required) ---
        if (typesize == null) {
            throw new IllegalArgumentException("TypeSize is required (not found in parameters)");
        }
        if (typesize instanceof Number) {
            this.typeSize = ((Number) typesize).intValue();
        } else if (typesize instanceof String) {
            try {
                this.typeSize = Integer.parseInt((String) typesize);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("TypeSize not supported : " + typesize);
            }
        } else {
            throw new IllegalArgumentException("TypeSize is required.");
        }

        // --- Handle blockSize (Required) ---
        if (blocksize == null) {
            throw new IllegalArgumentException("BlockSize is required (not found in parameters)");
        }
        if (blocksize instanceof Number) {
            this.blockSize = ((Number) blocksize).intValue();
        } else if (blocksize instanceof String) {
            try {
                this.blockSize = Integer.parseInt((String) blocksize);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("BlockSize not supported : " + blocksize);
            }
        } else {
            throw new IllegalArgumentException("BlockSize is required.");
        }

        // --- Handle Compressor ---
        if (cname == null) {
            this.compressor = Blosc.Compressor.ZSTD;
        } else if (cname instanceof String) {
            String str = (String) cname;
            switch (str) {
                case "lz4":      this.compressor = Blosc.Compressor.LZ4; break;
                case "lz4hc":    this.compressor = Blosc.Compressor.LZ4HC; break;
                case "blosclz":  this.compressor = Blosc.Compressor.BLOSCLZ; break;
                case "zlib":     this.compressor = Blosc.Compressor.ZLIB; break;
                case "zstd":     this.compressor = Blosc.Compressor.ZSTD; break;
                default:         throw new IllegalArgumentException("Compressor not supported : " + str);
            }
        }

        // --- Handle Shuffle ---
        if (shuffle == null) {
            this.shuffle = (this.typeSize == 1 ? Blosc.Shuffle.BIT_SHUFFLE : Blosc.Shuffle.BYTE_SHUFFLE);
        } else if (shuffle instanceof String) {
            String str = (String) shuffle;
            switch (str) {
                case "noshuffle":  this.shuffle = Blosc.Shuffle.NO_SHUFFLE; break;
                case "shuffle":    this.shuffle = Blosc.Shuffle.BYTE_SHUFFLE; break;
                case "bitshuffle": this.shuffle = Blosc.Shuffle.BIT_SHUFFLE; break;
                default:           throw new IllegalArgumentException("Shuffle not supported : " + str);
            }
        }
    }

    /**
     * Computes the encoded type for Zstandard (ZSTD) codec.
     * This codec only supports bytes to bytes transformation.
     *
     * @param decodedType the input type, which must be bytes
     * @return the output type, which is also bytes
     */
    @Override
    public ZarrRepresentationType computeEncodedType(ZarrRepresentationType decodedType) {
        // bytes->bytes: input must be bytes, output is bytes
        if (!decodedType.isBytes()) {
            throw new IllegalArgumentException("BLOSC codec requires input to be bytes");
        }
        return ZarrRepresentationType.bytes();
    }

    /**
     * Encode a value using Blosc.
     *
     * @param decodedValue the value to encode, expected to be a byte[] or ByteBuffer
     * @param decodedType not used here, set it to null or empty
     * @return the compressed value as a byte[]
     */
    @Override
    public Object encode(Object decodedValue, ZarrRepresentationType decodedType) {
        byte[] input;
        if (decodedValue instanceof byte[]) {
            input = (byte[]) decodedValue;
        } else if (decodedValue instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) decodedValue;
            if (buf.hasArray()) {
                input = buf.array();
            } else {
                input = new byte[buf.remaining()];
                buf.get(input);
            }
        } else {
            throw new IllegalArgumentException("Unsupported input: " + decodedValue.getClass());
        }

        return Blosc.compress(input, typeSize, compressor, level, shuffle, blockSize);
    }

    /**
     * Decode a compressed value using Blosc.
     *
     * @param encodedValue the compressed value, expected to be a byte[] or ByteBuffer
     * @param decodedType the expected decoded type, which may include shape and dtype information
     * @return the decompressed value as a ByteBuffer
     */
    @Override
    public Object decode(Object encodedValue, ZarrRepresentationType decodedType) {
        byte[] input;
        if (encodedValue instanceof byte[]) {
            input = (byte[]) encodedValue;
        } else if (encodedValue instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) encodedValue;
            if (buf.hasArray()) {
                input = buf.array();;
            } else {
                input = new byte[buf.remaining()];
                buf.get(input);
            }
        } else {
            throw new IllegalArgumentException("Unsupported input to blosc.decode: " + encodedValue.getClass());
        }
        try {
            // The output array size is detected by Blosc
            // Blosc does not support ByteBuffer
            return ByteBuffer.wrap(Blosc.decompress(input, 1));
        } catch (Exception ex) {
            throw new RuntimeException("Blosc decompression failed: " + ex, ex);
        }
    }
}
