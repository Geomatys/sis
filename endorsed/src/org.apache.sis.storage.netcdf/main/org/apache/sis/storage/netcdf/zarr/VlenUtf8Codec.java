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
        byte[] all = (bytes instanceof ByteBuffer) ? BytesCodec.getBytes((ByteBuffer) bytes) : (byte[]) bytes;
        int[] shape = decodedType.shape();
        int count = 1;
        for (int s : shape) count *= s;
        return decodeStringsVlenUtf8(all, count);
    }

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

    private static String[] decodeStringsVlenUtf8(byte[] payload, int count) throws DataStoreContentException {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int[] offsets = new int[count + 1];
        for (int i = 0; i < count + 1; i++) offsets[i] = buf.getInt();
        byte[] data = new byte[payload.length - 4 * (count + 1)];
        buf.get(data);

        String[] out = new String[count];
        for (int i = 0; i < count; i++) {
            int from = offsets[i];
            int to = offsets[i + 1];
            if (from == to) {
                out[i] = "";
            } else {
                out[i] = new String(data, from, to - from, StandardCharsets.UTF_8);
            }
        }
        return out;
    }
}
