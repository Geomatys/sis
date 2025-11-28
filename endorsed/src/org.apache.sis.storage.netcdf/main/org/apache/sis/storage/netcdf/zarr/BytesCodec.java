package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.util.resources.Errors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Bytes codec for Zarr datasets (ZarrCodec.BYTES).
 *
 * @author Quentin Bialota (Geomatys)
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
     * 
     * @param configuration the configuration parameters for the codec, which may include "endian" to specify byte order
     */
    public BytesCodec(Map<String, Object> configuration) {
        super(CODEC, configuration);

        String endian = configuration != null ? (String) configuration.getOrDefault("endian", "little") : "little";
        this.byteOrder = "big".equalsIgnoreCase(endian) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    /**
     * Computes the encoded type for the BYTES codec.
     * 
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
     * @param array       the value to encode (e.g., an array of numbers, booleans, etc.)
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
     * Decodes a byte[] or ByteBuffer into a ByteBuffer with the correct byte order.
     * No typed array is allocated — the returned ByteBuffer wraps the raw bytes
     * and can be used with {@link #decodeRegion} to extract only the needed elements.
     *
     * @param bytes       the encoded value, expected to be a byte[] or ByteBuffer
     * @param decodedType the representation type of the output array, which includes shape and data type
     * @return the ByteBuffer with correct byte order set
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
            throw new IllegalArgumentException("Unsupported input: " + bytes.getClass());
        }

        // Enforce the correct endianness for the view
        buf.order(byteOrder);

        return buf;
    }

    /**
     * Returns the byte order used by this codec for encoding/decoding.
     *
     * @return the byte order
     */
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

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
    @Override
    public void decodeRegion(ByteBuffer src, int srcPos, Object dst, int dstPos,
            int count, int step, DataType type) throws DataStoreException {
        final int typeSize = type.size();
        switch (type.number) {
            case BYTE: {
                byte[] d = (byte[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate();
                    slice.position(srcPos);
                    slice.get(d, dstPos, count);
                } else {
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = src.get(srcPos + k);
                    }
                }
                break;
            }
            case SHORT: {
                short[] d = (short[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asShortBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asShortBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case CHARACTER: {
                char[] d = (char[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asCharBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asCharBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case INTEGER: {
                int[] d = (int[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asIntBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asIntBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case LONG: {
                long[] d = (long[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asLongBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asLongBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case FLOAT: {
                float[] d = (float[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asFloatBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asFloatBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case DOUBLE: {
                double[] d = (double[]) dst;
                if (step == 1) {
                    ByteBuffer slice = src.duplicate().order(src.order());
                    slice.position(srcPos * typeSize);
                    slice.asDoubleBuffer().get(d, dstPos, count);
                } else {
                    var view = src.duplicate().order(src.order()).asDoubleBuffer();
                    for (int k = 0; k < count; k += step) {
                        d[dstPos++] = view.get(srcPos + k);
                    }
                }
                break;
            }
            case BOOLEAN: {
                boolean[] d = (boolean[]) dst;
                for (int k = 0; k < count; k += step) {
                    d[dstPos++] = src.get(srcPos + k) != 0;
                }
                break;
            }
            default:
                throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }

    /**
     * Converts an array to bytes and writes it to a ByteBuffer.
     *
     * @param buf   the ByteBuffer to write to
     * @param array the array to convert (e.g., byte[], short[], int[], etc.)
     * @param type  the data type of the array elements
     * @throws DataStoreContentException if the data type is unknown or unsupported
     */
    private static void toBytes(ByteBuffer buf, Object array, DataType type) throws DataStoreContentException {
        switch (type.number) {
            case BYTE: buf.put((byte[]) array);
                break;
            case SHORT: buf.asShortBuffer().put((short[]) array);
                break;
            case CHARACTER: buf.asCharBuffer().put((char[]) array);
                break;
            case INTEGER: buf.asIntBuffer().put((int[]) array);
                break;
            case LONG: buf.asLongBuffer().put((long[]) array);
                break;
            case FLOAT: buf.asFloatBuffer().put((float[]) array);
                break;
            case DOUBLE: buf.asDoubleBuffer().put((double[]) array);
                break;
            case BOOLEAN:
                for (boolean v : (boolean[]) array) buf.put((byte) (v ? 1 : 0));
                break;
            default:
                throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }

    /**
     * Computes the length of a 1D array based on its type.
     * 
     * @param array the array whose length is to be computed
     * @return the length of the array
     */
    private static int computeLength(Object array) throws DataStoreContentException {
        if (array instanceof byte[]) return ((byte[]) array).length;
        if (array instanceof short[]) return ((short[]) array).length;
        if (array instanceof int[]) return ((int[]) array).length;
        if (array instanceof long[]) return ((long[]) array).length;
        if (array instanceof float[]) return ((float[]) array).length;
        if (array instanceof double[]) return ((double[]) array).length;
        if (array instanceof boolean[]) return ((boolean[]) array).length;
        if (array instanceof char[]) return ((char[]) array).length;
        throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, array.getClass().getName()));
    }

    /**
     * Allocates a 1D array of the specified data type and length.
     *
     * @param type   the data type of the array elements
     * @param length the length of the array
     * @return a new array of the specified type and length
     * @throws DataStoreContentException if the data type is unknown or unsupported
     */
    public static Object allocate1DArray(DataType type, int length) throws DataStoreContentException {
        if (type == DataType.STRING) {
            return new String[length];
        }
        switch (type.number) {
            case BYTE: return new byte[length];
            case SHORT: return new short[length];
            case CHARACTER: return new char[length];
            case INTEGER: return new int[length];
            case LONG: return new long[length];
            case FLOAT: return new float[length];
            case DOUBLE: return new double[length];
            case BOOLEAN: return new boolean[length];
            default: throw new DataStoreContentException(Errors.format(Errors.Keys.UnknownType_1, type));
        }
    }
}
