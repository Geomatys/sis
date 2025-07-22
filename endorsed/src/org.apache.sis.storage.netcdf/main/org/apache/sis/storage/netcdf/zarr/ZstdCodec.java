package org.apache.sis.storage.netcdf.zarr;

import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Zstandard (ZSTD) codec for Zarr.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class ZstdCodec extends AbstractZarrCodec {

    /**
     * The Zarr codec type for Zstandard (ZSTD).
     */
    private static final ZarrCodec CODEC = ZarrCodec.ZSTD;

    /**
     * Compression level for Zstandard (ZSTD).
     */
    private final int level;

    /**
     * Constructor for ZstdCodec.
     * @param configuration the configuration parameters for the codec, which may include "level" to specify compression level
     */
    public ZstdCodec(Map<String, Object> configuration) {
        super(CODEC, configuration);

        // Default compression level = 1 as in Zarr spec
        Object l = configuration != null ? configuration.get("level") : null;
        int lvl = 1;
        if (l instanceof Number) {
            lvl = ((Number) l).intValue();
        } else if (l instanceof String) {
            try {
                lvl = Integer.parseInt((String) l);
            } catch (NumberFormatException e) { /* Ignore; use default */ }
        }
        this.level = lvl;
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
            throw new IllegalArgumentException("ZSTD codec requires input to be bytes");
        }
        return ZarrRepresentationType.bytes();
    }

    /**
     * Encode a value using Zstandard (ZSTD).
     *
     * @param decodedValue the value to encode, expected to be a byte[] or ByteBuffer
     * @param decodedType not used here, set it to null or empty
     * @return the compressed value as a byte[]
     */
    @Override
    public Object encode(Object decodedValue, ZarrRepresentationType decodedType) {
        // decodedValue is a byte[] or ByteBuffer
        byte[] input;
        if (decodedValue instanceof byte[]) {
            input = (byte[]) decodedValue;
        } else if (decodedValue instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) decodedValue;
            input = new byte[buf.remaining()];
            buf.get(input);
        } else {
            throw new IllegalArgumentException("Unsupported input to ZSTD.encode: " + decodedValue.getClass());
        }
        return Zstd.compress(input, level);
    }

    /**
     * Decode a compressed value using Zstandard (ZSTD).
     *
     * @param encodedValue the compressed value, expected to be a byte[] or ByteBuffer
     * @param decodedType the expected decoded type, which may include shape and dtype information
     * @return the decompressed value as a byte[]
     */
    @Override
    public Object decode(Object encodedValue, ZarrRepresentationType decodedType) {
        // encodedValue is a compressed byte[] or ByteBuffer, decodedType tells us expected length **optional**
        byte[] input;
        if (encodedValue instanceof byte[]) {
            input = (byte[]) encodedValue;
        } else if (encodedValue instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) encodedValue;
            input = new byte[buf.remaining()];
            buf.get(input);
        } else {
            throw new IllegalArgumentException("Unsupported input to ZSTD.decode: " + encodedValue.getClass());
        }
        try {
            // The output array size is detected by Zstd
            return Zstd.decompress(input, (int) Zstd.getFrameContentSize(input));
        } catch (Exception ex) {
            throw new RuntimeException("ZSTD decompression failed: " + ex, ex);
        }
    }
}
