package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.util.Numbers;
import org.apache.sis.util.resources.Errors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Bytes codec for Zarr datasets (ZarrCodec.BYTES).
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class BytesCodec extends AbstractZarrCodec {

    /**
     * The Zarr codec type for Bytes.
     */
    private static final ZarrCodec CODEC = ZarrCodec.BYTES;

    /**
     * Endianness of the byte order for encoding/decoding bytes.
     */
    private final ByteOrder byteOrder;

    /**
     * Constructor for BytesCodec.
     * @param configuration the configuration parameters for the codec, which may include "endian" to specify byte order
     */
    public BytesCodec(Map<String, Object> configuration) {
        super(CODEC, configuration);

        String endian = configuration != null ? (String) configuration.getOrDefault("endian", "little") : "little";
        this.byteOrder = "big".equalsIgnoreCase(endian) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    /**
     * Computes the encoded type for the BYTES codec.
     * @param decodedType Array info: shape, data type, etc. (you may want a struct for this!)
     * @return the output type after encoding, which is always bytes for BYTES codec
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
        // "Flatten" the array to bytes
        int length = computeLength(array);
        ByteBuffer buf = ByteBuffer.allocate(length * decodedType.dtype().size()).order(byteOrder);
        toBytes(buf, array, decodedType.dtype());
        buf.flip();
        return buf.array();
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
        byte[] b = (bytes instanceof ByteBuffer) ? getBytes((ByteBuffer) bytes) : (byte[]) bytes;
        return fromBytes(b, decodedType.shape(), decodedType.dtype(), byteOrder);
    }

    /**
     * Converts an array to bytes and writes it to a ByteBuffer.
     *
     * @param buf the ByteBuffer to write to
     * @param array the array to convert (e.g., byte[], short[], int[], etc.)
     * @param type the data type of the array elements
     * @throws DataStoreContentException if the data type is unknown or unsupported
     */
    private static void toBytes(ByteBuffer buf, Object array, DataType type) throws DataStoreContentException {
        switch (type.number) {
            case Numbers.BYTE:  buf.put((byte[]) array); break;
            case Numbers.SHORT: buf.asShortBuffer().put((short[]) array); break;
            case Numbers.CHARACTER: buf.asCharBuffer().put((char[]) array); break;
            case Numbers.INTEGER: buf.asIntBuffer().put((int[]) array); break;
            case Numbers.LONG: buf.asLongBuffer().put((long[]) array); break;
            case Numbers.FLOAT: buf.asFloatBuffer().put((float[]) array); break;
            case Numbers.DOUBLE: buf.asDoubleBuffer().put((double[]) array); break;
            case Numbers.BOOLEAN: for (boolean v : (boolean[]) array) buf.put((byte)(v ? 1 : 0)); break;
            default: throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }

    /**
     * Converts a byte array to an array of the specified shape and type.
     *
     * @param b the byte array to convert
     * @param shape the shape of the output array (e.g., [3, 4] for a 2D array)
     * @param type the data type of the output array elements
     * @param order the byte order (endianness) to use for decoding
     * @return the decoded array (e.g., byte[], short[], int[], etc.)
     * @throws DataStoreContentException if the data type is unknown or unsupported
     */
    private static Object fromBytes(byte[] b, int[] shape, DataType type, ByteOrder order) throws DataStoreContentException {
        int len = 1;
        for (int s : shape) len *= s;
        ByteBuffer buf = ByteBuffer.wrap(b).order(order);

        switch (type.number) {
            case Numbers.BYTE:  byte[] xb = new byte[len]; buf.get(xb); return xb;
            case Numbers.SHORT: short[] s = new short[len]; buf.asShortBuffer().get(s); return s;
            case Numbers.CHARACTER: char[]  xc = new char[len]; buf.asCharBuffer().get(xc); return xc;
            case Numbers.INTEGER: int[]  xi = new int[len]; buf.asIntBuffer().get(xi); return xi;
            case Numbers.LONG: long[] xl = new long[len]; buf.asLongBuffer().get(xl); return xl;
            case Numbers.FLOAT: float[] xf = new float[len]; buf.asFloatBuffer().get(xf); return xf;
            case Numbers.DOUBLE: double[] xd = new double[len]; buf.asDoubleBuffer().get(xd); return xd;
            case Numbers.BOOLEAN: boolean[] xbval = new boolean[len]; for (int i = 0; i < len; i++) xbval[i] = buf.get() != 0; return xbval;
            default: throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }

    /**
     * Extracts bytes from a ByteBuffer, handling both array-backed and non-array-backed buffers.
     *
     * @param buf the ByteBuffer from which to extract bytes
     * @return a byte array containing the bytes from the buffer
     */
    static byte[] getBytes(ByteBuffer buf) {
        if (buf.hasArray()) {
            int start = buf.position();
            int len = buf.remaining();
            byte[] arr = new byte[len];
            System.arraycopy(buf.array(), buf.arrayOffset() + start, arr, 0, len);
            return arr;
        } else {
            byte[] arr = new byte[buf.remaining()];
            buf.get(arr);
            return arr;
        }
    }

    /**
     * Computes the length of a 1D array based on its type.
     * @param array the array whose length is to be computed
     * @return the length of the array
     */
    private static int computeLength(Object array) throws DataStoreContentException {
        if (array instanceof byte[])   return ((byte[]) array).length;
        if (array instanceof short[])  return ((short[]) array).length;
        if (array instanceof int[])    return ((int[]) array).length;
        if (array instanceof long[])   return ((long[]) array).length;
        if (array instanceof float[])  return ((float[]) array).length;
        if (array instanceof double[]) return ((double[]) array).length;
        if (array instanceof boolean[]) return ((boolean[]) array).length;
        if (array instanceof char[])  return ((char[]) array).length;
        throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, array.getClass().getName()));
    }

    /**
     * Allocates a 1D array of the specified data type and length.
     *
     * @param type the data type of the array elements
     * @param length the length of the array
     * @return a new array of the specified type and length
     * @throws DataStoreContentException if the data type is unknown or unsupported
     */
    public static Object allocate1DArray(DataType type, int length) throws DataStoreContentException {
        if (type == DataType.STRING) {
            return new String[length];
        }
        switch (type.number) {
            case Numbers.BYTE:  return new byte[length];
            case Numbers.SHORT: return new short[length];
            case Numbers.CHARACTER: return new char[length];
            case Numbers.INTEGER: return new int[length];
            case Numbers.LONG: return new long[length];
            case Numbers.FLOAT: return new float[length];
            case Numbers.DOUBLE: return new double[length];
            case Numbers.BOOLEAN: return new boolean[length];
            default: throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }
}
