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
package org.apache.sis.internal.storage.inflater;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.sis.internal.storage.io.ChannelDataInput;


/**
 * Inflater for values encoded with the CCITT Group 3, 1-Dimensional Modified Huffman run length encoding.
 * This compression is described in section 10 of TIFF 6 specification. "Run length" (consecutive black or
 * white pixels) are encoded with "words" having a variable number of bits. Example:
 *
 * <table class="sis">
 *   <caption>Run length encoding examples</caption>
 *   <tr><th>Run length</th>  <th>Encoding</th></tr>
 *   <tr><td>0</td> <td>0000110111</td></tr>
 *   <tr><td>1</td> <td>010</td></tr>
 *   <tr><td>2</td> <td>11</td></tr>
 *   <tr><td>3</td> <td>10</td></tr>
 *   <tr><td>4</td> <td>011</td></tr>
 *   <tr><td>5</td> <td>0011</td></tr>
 * </table>
 *
 * Because the number of bits varies, leading zeros are significant. For example "11" is not equivalent to "011",
 * which is not equivalent to "0011" neither. Consequently we can not parse directly the bits as integer values.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 * @since   1.1
 * @module
 */
final class CCITTRLE extends CompressionChannel {
    /**
     * Modified Huffman tree for length of runs of white and black colors. This array is generated by
     * the {@code CCITTRLETest} class, which will also verifies that those values are still corrects.
     * This array is used for finding the "run length" associated to a sequence of bits like below:
     *
     * <ul>
     *   <li>If value at offset <var>i</var> is negative, then the result is {@code ~RUNLENGTH_TREE[i]}.</li>
     *   <li>Otherwise read a bit and choose a branch according the bit value:<ul>
     *     <li>If the bit is 0, continue tree traversal at <var>i</var>+1.</li>
     *     <li>If the bit is 1, continue tree traversal at {@code RUNLENGTH_TREE[i]}.</li></ul>
     *   </li>
     * </ul>
     */
    private static final short[] WHITE_RUNLENGTH_TREE = {
        184, 107, 70, 49, 42, 37, 34, 9, 0, 23, 16, 13, ~1792, 15, ~1984, ~2048, 20, 19, ~2112, ~2176, 22, ~2240,
        ~2304, 27, 26, ~1856, ~1920, 31, 30, ~2368, ~2432, 33, ~2496, ~2560, 36, ~29, ~30, 41, 40, ~45, ~46,
        ~22, 48, 45, ~23, 47, ~47, ~48, ~13, 63, 56, 53, ~20, 55, ~33, ~34, 60, 59, ~35, ~36, 62, ~37, ~38,
        69, 66, ~19, 68, ~31, ~32, ~1, 92, 79, 74, ~12, 78, 77, ~53, ~54, ~26, 87, 84, 83, ~39, ~40, 86, ~41,
        ~42, 91, 90, ~43, ~44, ~21, 106, 99, 96, ~28, 98, ~61, ~62, 103, 102, ~63, ~0, 105, ~320, ~384, ~10,
        147, 126, 111, ~11, 117, 114, ~27, 116, ~59, ~60, 125, 122, 121, ~1472, ~1536, 124, ~1600, ~1728, ~18,
        138, 133, 130, ~24, 132, ~49, ~50, 137, 136, ~51, ~52, ~25, 146, 143, 142, ~55, ~56, 145, ~57, ~58,
        ~192, 183, 160, 151, ~1664, 155, 154, ~448, ~512, 159, 158, ~704, ~768, ~640, 174, 167, 164, ~576, 166,
        ~832, ~896, 171, 170, ~960, ~1024, 173, ~1088, ~1152, 182, 179, 178, ~1216, ~1280, 181, ~1344, ~1408,
        ~256, ~2, 198, 191, 188, ~3, 190, ~128, ~8, 197, 194, ~9, 196, ~16, ~17, ~4, 206, 201, ~5, 205, 204,
        ~14, ~15, ~64, 208, ~6, ~7
    },
    BLACK_RUNLENGTH_TREE = {
        206, 203, 200, 195, 152, 101, 34, 9, 0, 23, 16, 13, ~1792, 15, ~1984, ~2048, 20, 19, ~2112, ~2176, 22,
        ~2240, ~2304, 27, 26, ~1856, ~1920, 31, 30, ~2368, ~2432, 33, ~2496, ~2560, 68, 49, 38, ~18, 44, 41,
        ~52, 43, ~640, ~704, 48, 47, ~768, ~832, ~55, 61, 56, 53, ~56, 55, ~1280, ~1344, 60, 59, ~1408, ~1472,
        ~59, 67, 64, ~60, 66, ~1536, ~1600, ~24, 86, 77, 72, ~25, 76, 75, ~1664, ~1728, ~320, 81, 80, ~384,
        ~448, 85, 84, ~512, ~576, ~53, 100, 93, 90, ~54, 92, ~896, ~960, 97, 96, ~1024, ~1088, 99, ~1152, ~1216,
        ~64, 127, 104, ~13, 118, 111, 108, ~23, 110, ~50, ~51, 115, 114, ~44, ~45, 117, ~46, ~47, 126, 123,
        122, ~57, ~58, 125, ~61, ~256, ~16, 151, 138, 131, ~17, 135, 134, ~48, ~49, 137, ~62, ~63, 146, 143,
        142, ~30, ~31, 145, ~32, ~33, 150, 149, ~40, ~41, ~22, ~14, 156, 155, ~10, ~11, 194, 173, 160, ~15,
        168, 165, 164, ~128, ~192, 167, ~26, ~27, 172, 171, ~28, ~29, ~19, 187, 180, 177, ~20, 179, ~34, ~35,
        184, 183, ~36, ~37, 186, ~38, ~39, 193, 190, ~21, 192, ~42, ~43, ~0, ~12, 199, 198, ~9, ~8, ~7, 202,
        ~6, ~5, 205, ~1, ~4, 208, ~3, ~2
    };

    /**
     * Value after the last terminating run length. Run lengths equal or greater to this value
     * must be added to the next value until a terminating value is found.
     */
    static final int TERMINATING_LIMIT = 64;

    /**
     * Number of bits in an image row, ignoring sub-regions and subsampling.
     */
    private final int bitsPerRow;

    /**
     * Number of bits that we still have to read for current image row.
     */
    private int remainingBitsInRow;

    /**
     * {@code true} if reading black color, or {@code false} if reading white color.
     * The photometric interpretation ("White is zero" versus "Black is zero") does
     * not need to be handled here; it is handled by the color model instead.
     */
    private boolean runIsWhite;

    /**
     * Number of black or white pixels to write in the destination buffer.
     */
    private int runLength;

    /**
     * Creates a new channel which will decompress data from the given input.
     * The {@link #setInputRegion(long, long)} method must be invoked after construction
     * before a reading process can start.
     *
     * @param  input        the source of data to decompress.
     * @param  sourceWidth  number of pixels in a row of the source image.
     */
    CCITTRLE(final ChannelDataInput input, final int sourceWidth) {
        super(input);
        bitsPerRow = sourceWidth;
    }

    /**
     * Prepares this inflater for reading a new tile or a new band of a tile.
     *
     * @param  start      stream position where to start reading.
     * @param  byteCount  number of byte to read from the input.
     * @throws IOException if the stream can not be seek to the given start position.
     */
    @Override
    public void setInputRegion(final long start, final long byteCount) throws IOException {
        super.setInputRegion(start, byteCount);
        remainingBitsInRow = bitsPerRow;
        runIsWhite = false;
        runLength  = 0;
    }

    /**
     * Returns the number of bits of white pixels or black pixels to append.
     *
     * @todo We could improve efficiency by reading directly the 4 first bits in white case,
     *       or the 2 first bits in the black case, instead of reading all bits one by one.
     *       It is not sure that CCITTRLE compression is used widely enough to be worth optimizations.
     *
     * @param  tree  {@link #WHITE_RUNLENGTH_TREE} or {@link #BLACK_RUNLENGTH_TREE}.
     */
    final int getRunLength(final short[] tree) throws IOException {
        int runLength = 0, code;
        do {
            int offset = 0;
            while ((code = tree[offset]) >= 0) {
                if (code == 0) {
                    /*
                     * This case can occur only when reading 00000000 (verified by JUnit tests),
                     * which is the prefix for End-Of-Line (EOL) word.
                     *
                     *   Prefix:     00000000
                     *   White EOL:  000000000001
                     *   Black EOL:  00000000000
                     *
                     * EOL is not allowed by TIFF specification, but we check anyway for safety.
                     */
                    final long bits = input.readBits(runIsWhite ? 4 : 3);
                    if (bits != (runIsWhite ? 1 : 0)) {
                        throw new IOException("Unexpected EOL");
                    }
                    input.skipRemainingBits();
                    runIsWhite = false;
                    return remainingBitsInRow;
                }
                offset = (input.readBit() != 0) ? code : offset + 1;
            }
            code = ~code;
            runLength += code;
        } while (code >= TERMINATING_LIMIT);
        return runLength;
    }

    /**
     * Decompresses some bytes from the {@linkplain #input} into the given destination buffer.
     *
     * @param  target  the buffer into which bytes are to be transferred.
     * @return the number of bytes read, or -1 if end-of-stream.
     * @throws IOException if some other I/O error occurs.
     */
    @Override
    public int read(final ByteBuffer target) throws IOException {
        final int start    = target.position();
        int pendingByte    = 0;
        int numPendingBits = 0;
        while (target.hasRemaining()) {
            while (runLength == 0) {
                /*
                 * If we reached the end of a row, we need to skip unused bits because
                 * TIFF specification said that each new row starts on a byte boundary.
                 */
                if (remainingBitsInRow <= 0) {
                    remainingBitsInRow = bitsPerRow;
                    input.skipRemainingBits();
                    if (numPendingBits != 0) {
                        target.put((byte) (pendingByte << (Byte.SIZE - numPendingBits)));
                    }
                    numPendingBits = 0;
                    runIsWhite = false;
                }
                /*
                 * If we reached the end of compressed data, it is an error. But instead of
                 * declaring EOF, fill the remaining of the row with black or white values.
                 */
                if (finished()) {
                    final int n = target.position() - start;
                    if (n > 0) return n;
                    runLength = remainingBitsInRow;
                } else {
                    runIsWhite = !runIsWhite;
                    runLength = getRunLength(runIsWhite ? WHITE_RUNLENGTH_TREE : BLACK_RUNLENGTH_TREE);
                }
                remainingBitsInRow -= runLength;
            }
            /*
             * If we were in the middle of writing a byte, complete that byte by appending the new bits.
             * If the 8 bits become set, we can write to target. Otherwise we have to wait for next iteration.
             */
            if (numPendingBits != 0) {
                final int n = Math.min(Byte.SIZE - numPendingBits, runLength);
                pendingByte <<= n;
                if (!runIsWhite) {
                    pendingByte |= (1 << n) - 1;
                }
                if ((numPendingBits += n) >= Byte.SIZE) {
                    target.put((byte) pendingByte);
                    numPendingBits = 0;
                }
                runLength -= n;
            } else {
                /*
                 * Write multiples of 8 bits as complete bytes, then put the remaining bits in
                 * the "pending" byte, to be completed with opposite color in next iteration.
                 *
                 * Note: The "normal" `PhotometricInterpretation` for bilevel CCITT compressed
                 *       data is `WhiteIsZero`.
                 */
                final int  r = target.remaining();
                final int  n = Math.min(runLength >>> SIZE_SHIFT, r);
                final byte b = runIsWhite ? 0 : (byte) 0xFF;
                repeat(target, b, n);
                runLength -= n << SIZE_SHIFT;
                if (n == r) {
                    break;      // If we filled target buffer, do not lost bits in `pendingByte`.
                }
                if ((runLength & ~(Byte.SIZE - 1)) == 0) {
                    numPendingBits = runLength;
                    pendingByte = b & ((1 << numPendingBits) - 1);
                    runLength = 0;
                }
            }
        }
        assert numPendingBits == 0 : numPendingBits;
        return target.position() - start;
    }

    /**
     * Bit shift to apply on 1 for obtaining {@value Byte#SIZE} (2³ = 8).
     */
    private static final int SIZE_SHIFT = 3;
}
