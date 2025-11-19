package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.util.Numbers;
import org.apache.sis.util.resources.Errors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Vlen-UTF8 codec for Zarr datasets (ZarrCodec.VLEN_UTF8).
 * Zarr 3 extension
 * Source : <a href="https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/vlen-utf8/README.md">VlenUTF8 codec</a>
 *
 * @author Quentin Bialota (Geomatys)
 */
final class VlenUtf8Codec extends AbstractZarrCodec {

    /**
     * The Zarr codec type for Vlen_Utf8.
     */
    private static final ZarrCodec CODEC = ZarrCodec.VLEN_UTF8;

    /**
     * Constructor for BytesCodec.
     * @param configuration the configuration parameters for the codec, which may include "endian" to specify byte order
     */
    public VlenUtf8Codec(Map<String, Object> configuration) {
        super(CODEC, configuration);
    }

    /**
     * Computes the encoded type for the VLEN_UTF8 codec.
     * @param decodedType Array info: shape, data type, etc. (you may want a struct for this!)
     * @return the output type after encoding, which is always bytes for VLEN_UTF8 codec
     */
    @Override
    public ZarrRepresentationType computeEncodedType(ZarrRepresentationType decodedType) {
        // array -> bytes
        if (decodedType.isBytes()) {
            throw new IllegalArgumentException("Decoded type is already bytes.");
        }
        return ZarrRepresentationType.bytes();
    }

    /**
     * Encodes an array into a byte[] representation.
     *
     * @param array the value to encode (e.g., an array of numbers, booleans, etc.)
     * @param decodedType the representation type of the input array, which includes shape and data type
     * @return the encoded value as a byte[]
     */
    @Override
    public Object encode(Object array, ZarrRepresentationType decodedType) throws DataStoreContentException {
        if (array == null) return null;

        String[] src;
        if (array instanceof String[]) {
            src = (String[]) array;
        } else if (array instanceof List<?>) {
            List<?> list = (List<?>) array;
            src = list.toArray(new String[0]);
        } else {
            throw new DataStoreContentException("VlenUtf8Codec: Unsupported input, must be String[] or List<String>");
        }
        return encodeStringsVlenUtf8(src);
    }

    /**
     * Decodes a byte[] or ByteBuffer into an array representation.
     *
     * @param bytes the encoded value, expected to be a byte[] or ByteBuffer
     * @param decodedType the representation type of the output array, which includes shape and data type
     * @return the decoded value as an array (e.g., byte[], short[], int[], etc.)
     */
    @Override
    public Object decode(Object bytes, ZarrRepresentationType decodedType) throws DataStoreContentException {
        if (bytes == null) return null;

        ByteBuffer buf;
        if (bytes instanceof ByteBuffer) {
            buf = (ByteBuffer) bytes;
        } else if (bytes instanceof byte[]) {
            buf = ByteBuffer.wrap((byte[]) bytes);
        } else {
            throw new DataStoreContentException("Unsupported input type for VlenUtf8Codec");
        }

        // VLenUTF8 is always Little Endian for the offsets
        buf.order(ByteOrder.LITTLE_ENDIAN);

        int[] shape = decodedType.shape();
        int count = 1;
        for (int s : shape) count *= s;

        return decodeStringsVlenUtf8(buf, count);
    }

    /**
     * Encodes an array of strings into VLen-UTF8 byte representation.
     * @param strings the array of strings to encode
     * @return the encoded byte array in VLen-UTF8 format
     */
    private static byte[] encodeStringsVlenUtf8(String[] strings) {
        List<byte[]> utf8s = new ArrayList<>(strings.length);
        int[] offsets = new int[strings.length + 1];
        int total = 0;
        for (int i = 0; i < strings.length; i++) {
            byte[] bytes = (strings[i] != null) ? strings[i].getBytes(StandardCharsets.UTF_8) : new byte[0];
            utf8s.add(bytes);
            total += bytes.length;
            offsets[i + 1] = total;
        }
        ByteBuffer buf = ByteBuffer.allocate(4 * (strings.length + 1) + total).order(ByteOrder.LITTLE_ENDIAN);
        for (int off : offsets) buf.putInt(off);
        for (byte[] bytes : utf8s) buf.put(bytes);
        return buf.array();
    }

    /**
     * Decodes VLen-UTF8 byte representation into an array of strings.
     * @param buf the ByteBuffer containing VLen-UTF8 encoded data
     * @param count the number of strings to decode
     * @return the decoded array of strings
     */
    private static String[] decodeStringsVlenUtf8(ByteBuffer buf, int count) {
        String[] out = new String[count];
        int startPos = buf.position();

        // Calculate where the string data actually begins
        int headerByteSize = (count + 1) * Integer.BYTES;
        int dataStartPos = startPos + headerByteSize;

        // Fast Way : Heap ByteBuffer (Backed by byte[])
        if (buf.hasArray()) {
            byte[] arr = buf.array();
            int arrayOffset = buf.arrayOffset();

            for (int i = 0; i < count; i++) {
                // Read offset[i] and offset[i+1]
                int offStart = buf.getInt(startPos + (i * Integer.BYTES));
                int offEnd   = buf.getInt(startPos + ((i + 1) * Integer.BYTES));
                int len = offEnd - offStart;

                if (len == 0) {
                    out[i] = "";
                } else {
                    out[i] = new String(arr, arrayOffset + dataStartPos + offStart, len, StandardCharsets.UTF_8);
                }
            }
        }
        // Slow Way: Direct ByteBuffer (or Read-Only)
        else {
            // We cannot access the array directly, so we must copy the bytes for each string.

            for (int i = 0; i < count; i++) {
                int offStart = buf.getInt(startPos + (i * Integer.BYTES));
                int offEnd   = buf.getInt(startPos + ((i + 1) * Integer.BYTES));
                int len = offEnd - offStart;

                if (len == 0) {
                    out[i] = "";
                } else {
                    byte[] strBytes = new byte[len];
                    ByteBuffer view = buf.duplicate();
                    view.position(dataStartPos + offStart);
                    view.get(strBytes);
                    out[i] = new String(strBytes, StandardCharsets.UTF_8);
                }
            }
        }

        return out;
    }
}
