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

import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.netcdf.base.DataType;

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
     * Constructor for VlenUtf8Codec.
     *
     * @param configuration the configuration parameters for the codec, which may include "endian" to specify byte order
     */
    public VlenUtf8Codec(Map<String, Object> configuration) {
        super(CODEC, configuration);
    }

    /**
     * Computes the encoded type for the VLEN_UTF8 codec.
     *
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
     * @param array       the value to encode (e.g., an array of numbers, booleans, etc.)
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
     * Decodes a byte[] or ByteBuffer into a ByteBuffer with little-endian byte order.
     * No String[] is allocated — the returned ByteBuffer wraps the raw VLen-UTF8 bytes
     * and can be used with {@link #decodeRegion} to extract only the needed strings.
     *
     * @param bytes       the encoded value, expected to be a byte[] or ByteBuffer
     * @param decodedType the representation type of the output array, which includes shape and data type
     * @return the ByteBuffer with little-endian byte order set
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

        return buf;
    }

    /**
     * Decodes string elements from a VLen-UTF8 ByteBuffer directly into a String[] output array.
     * Only the requested elements are extracted — supports subsampling via the {@code step} parameter.
     * The VLen-UTF8 format consists of:
     *  <ul>
     *      <li>A header of {@code (totalCount + 1)} little-endian 32-bit integers representing byte offsets</li>
     *      <li>Followed by the concatenated UTF-8 string data</li>
     *  </ul>
     *
     * @param src    the source ByteBuffer containing VLen-UTF8 encoded data
     * @param srcPos the starting position in string index units (not bytes)
     * @param dst    the destination array (must be String[])
     * @param dstPos the starting position in the destination array
     * @param count  the number of source string elements to consider (before subsampling)
     * @param step   the subsampling step (1 = contiguous, &gt;1 = skip elements)
     * @param type   the data type (expected to be STRING)
     * @throws DataStoreException if an error occurs during decoding
     */
    @Override
    public void decodeRegion(ByteBuffer src, int srcPos, Object dst, int dstPos,
            int count, int step, DataType type) throws DataStoreException {
        String[] d = (String[]) dst;
        int startPos = src.position();

        // We need to know the total number of strings in the buffer to calculate the
        // header size.
        // The header contains (totalCount + 1) offsets. We can infer totalCount from
        // the first offset value:
        // offset[0] should be 0, and the header size = first_non_zero_position * 4, but
        // actually
        // the total count is passed implicitly via the chunk shape in the metadata.
        // However, we don't have that info here. Instead, we rely on the fact that the
        // offsets
        // table starts at startPos. The offset of string[i] in the data section is at:
        // startPos + i * 4
        // We need to know where the data section starts, which is at:
        // startPos + (totalStrings + 1) * 4
        // But we don't know totalStrings. We can find it by reading offset[0] which
        // gives us
        // the byte offset of the first string relative to the data section start.
        // Actually, all offsets are relative to the data section start, so offset[0]
        // should be 0.
        // The data section starts after (totalStrings + 1) * Integer.BYTES.
        // We can determine totalStrings from: the first offset entry value is 0,
        // and we know the position of strings we want. Since offsets are cumulative
        // byte offsets,
        // we just need the entries at positions [srcPos..srcPos+count/step] and
        // [srcPos+1..etc].
        // The data section offset can be computed if we know how many strings total
        // exist,
        // but that requires external knowledge.

        // Fortunately, for VLen-UTF8, the header size depends on the total number of
        // strings.
        // We can compute it: the first 4 bytes at the buffer should give offset[0] = 0.
        // Then each subsequent 4 bytes gives the cumulative byte length.
        // The headerByteSize cannot be determined without totalCount.
        // However, since this is called from copyChunkRegionToOutput which works with
        // chunk strides,
        // srcPos indexes into the flat chunk. The total number of strings in the chunk
        // can be inferred
        // from the chunk shape. But we don't have that here.

        // Pragmatic approach: infer the total count from the buffer structure.
        // The offset table has (N+1) entries. offset[0] = 0 always.
        // We can scan to find N: the data section starts where the first string byte
        // is.
        // But this is fragile. Better approach: reconstruct all strings and pick what
        // we need.

        // Decode all strings first (VLen format doesn't support random access
        // efficiently
        // without knowing the total count), then pick the requested region.
        // This is a reasonable tradeoff since string chunks are typically small.
        int bufSize = src.remaining();
        // Find total count by reading until we can determine the header/data boundary.
        // The first offset is 0, which means the data starts at position (N+1)*4.
        // We try to find N such that src.getInt(startPos + N*4) gives a valid
        // cumulative offset
        // and src.getInt(startPos + (N+1)*4) would be the start of actual UTF-8 data.
        // Actually, the most robust approach: decode all strings from the buffer, then
        // pick.
        String[] allStrings = decodeStringsVlenUtf8(src.duplicate().order(ByteOrder.LITTLE_ENDIAN),
                inferStringCount(src, startPos));

        // Now extract only the requested region with subsampling
        for (int k = 0; k < count; k += step) {
            int idx = srcPos + k;
            if (idx < allStrings.length) {
                d[dstPos++] = allStrings[idx];
            }
        }
    }

    /**
     * Infers the number of strings encoded in a VLen-UTF8 buffer.
     * The header contains (N+1) little-endian int32 offsets where offset[0] = 0.
     * We find N by looking for the first offset entry whose value, when interpreted
     * as
     * the data section start, is consistent with the buffer layout.
     *
     * @param buf      the ByteBuffer positioned at the start of VLen-UTF8 data
     * @param startPos the starting position in the buffer
     * @return the inferred number of strings
     */
    private static int inferStringCount(ByteBuffer buf, int startPos) {
        // The offset table has (N+1) entries of 4 bytes each.
        // offset[0] = 0, offset[N] = total bytes of all string data.
        // The data section starts at startPos + (N+1)*4.
        // Total buffer remaining from startPos = (N+1)*4 + totalStringBytes
        // And offset[N] = totalStringBytes
        // So: remaining = (N+1)*4 + offset[N]
        int remaining = buf.limit() - startPos;
        // Try increasing N until (N+1)*4 + getInt(startPos + N*4) == remaining
        for (int n = 0;; n++) {
            int headerSize = (n + 1) * Integer.BYTES;
            if (headerSize > remaining) {
                return Math.max(0, n - 1);
            }
            int lastOffset = buf.getInt(startPos + n * Integer.BYTES);
            if (headerSize + lastOffset == remaining) {
                return n;
            }
        }
    }

    /**
     * Encodes an array of strings into VLen-UTF8 byte representation.
     *
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
        for (int off : offsets)
            buf.putInt(off);
        for (byte[] bytes : utf8s)
            buf.put(bytes);
        return buf.array();
    }

    /**
     * Decodes VLen-UTF8 byte representation into an array of strings.
     *
     * @param buf   the ByteBuffer containing VLen-UTF8 encoded data
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
                int offEnd = buf.getInt(startPos + ((i + 1) * Integer.BYTES));
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
                int offEnd = buf.getInt(startPos + ((i + 1) * Integer.BYTES));
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
