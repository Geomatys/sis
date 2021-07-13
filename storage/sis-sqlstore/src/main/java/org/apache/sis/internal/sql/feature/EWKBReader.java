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
package org.apache.sis.internal.sql.feature;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.apache.sis.internal.feature.GeometryWrapper;
import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.util.resources.Errors;


/**
 * Reader of geometries encoded in Well Known Binary (WKB) format.
 * This class expects the WKB format as defined by OGC specification,
 * but the extended EWKB format (specific to PostGIS) is also accepted
 * if the {@link org.apache.sis.setup.GeometryLibrary} can handle it.
 *
 * <p>The WKB format is what we get from a spatial database (at least PostGIS)
 * when querying a geometry field without using any {@code ST_X} method.</p>
 *
 * <p>References:</p>
 * <ul>
 *   <li><a href="https://portal.ogc.org/files/?artifact_id=25355">OGC Simple feature access - Part 1: Common architecture</a></li>
 *   <li><a href="https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary">Well-known binary on Wikipedia</a></li>
 *   <li><a href="http://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT">PostGIS extended format</a></li>
 * </ul>
 *
 * @author  Johann Sorel (Geomatys)
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class EWKBReader<G> {
    /**
     * The factory to use for creating geometries from WKB definitions.
     */
    private final Geometries<G> geometryFactory;

    /**
     * The mapper to use for resolving a Spatial Reference Identifier (SRID) integer
     * as Coordinate Reference System (CRS) object.
     * This is {@code null} is there is no mapping to apply.
     */
    private CachedStatements fromSridToCRS;

    /**
     * The Coordinate Reference System if {@link #fromSridToCRS} can not map the SRID.
     * This is {@code null} if there is no default.
     */
    final CoordinateReferenceSystem defaultCRS;

    /**
     * Creates a new reader. The same instance can be reused for parsing an arbitrary
     * amount of geometries sharing the same CRS.
     *
     * @param  geometryFactory  the factory to use for creating geometries from WKB definitions.
     * @param  defaultCRS       the CRS to use if none can be mapped from the SRID, or {@code null} if none.
     * @return a WKB reader resolving SRID with the specified mapper and default CRS.
     */
    EWKBReader(final Geometries<G> geometryFactory, final CoordinateReferenceSystem defaultCRS) {
        this.geometryFactory = geometryFactory;
        this.defaultCRS      = defaultCRS;
    }

    /**
     * Sets the mapper to use for resolving a Spatial Reference Identifier (SRID) integer
     * as Coordinate Reference System (CRS) object.
     *
     * @param  fromSridToCRS  mapper for resolving SRID integers as CRS objects, or {@code null} if none.
     */
    final void setSridResolver(final CachedStatements fromSridToCRS) {
        this.fromSridToCRS = fromSridToCRS;
    }

    /**
     * Parses a WKB encoded as hexadecimal numbers in a character string.
     * Each byte uses 2 characters. No separator is allowed between bytes.
     *
     * @param  wkb  the hexadecimal values to decode. Should neither be null nor empty.
     * @return geometry parsed from the given hexadecimal text. Never null, never empty.
     * @throws Exception if the WKB can not be parsed. The exception type depends on the geometry implementation.
     */
    GeometryWrapper<G> readHexa(final String wkb) throws Exception {
        final int length = wkb.length();
        if ((length & 1) != 0) {
            throw new DataStoreContentException(Errors.format(Errors.Keys.OddArrayLength_1, "wkb"));
        }
        final byte[] data = new byte[length >>> 1];
        for (int i=0; i<length;) {
            data[i >>> 1] = (byte) ((digit(wkb.charAt(i++)) << 4) | digit(wkb.charAt(i++)));
        }
        return read(data);
    }

    /**
     * Returns the numerical value of the given hexadecimal digit.
     * The hexadecimal digit can be the decimal digits 0 to 9, or the letters A to F ignoring case.
     *
     * <div class="note"><b>Implementation note:</b>
     * we do not use {@link Character#digit(char, int)} because that method handled a large
     * range of Unicode characters, which is a wider scope than what is intended here.</div>
     *
     * @param  c  the hexadecimal digit.
     * @return numerical value of given digit.
     * @throws DataStoreContentException if the given character is not a hexadecimal digit.
     */
    private static int digit(final char c) throws DataStoreContentException {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'A' && c <= 'F') return c - ('A' - 10);
        if (c >= 'a' && c <= 'f') return c - ('a' - 10);
        throw new DataStoreContentException(Errors.format(Errors.Keys.CanNotParse_1, String.valueOf(c)));
    }

    /**
     * Parses a WKB stored in the given byte array.
     *
     * @param  wkb  the array containing the WKB to decode. Should neither be null nor empty.
     * @return geometry parsed from the given array of bytes. Never null, never empty.
     * @throws Exception if the WKB can not be parsed. The exception type depends on the geometry implementation.
     */
    GeometryWrapper<G> read(final byte[] wkb) throws Exception {
        return read(ByteBuffer.wrap(wkb));
    }

    /**
     * Parses a WKB stored in the given buffer of bytes.
     * Parsing starts at the {@linkplain ByteBuffer#position() buffer position}
     * and continue until the {@linkplain ByteBuffer#limit() buffer limit}.
     *
     * @param  wkb  the buffer containing the WKB to decode. Should neither be null nor empty.
     * @return geometry parsed from the given sequence of bytes. Never null, never empty.
     * @throws Exception if the WKB can not be parsed. The exception type depends on the geometry implementation.
     */
    GeometryWrapper<G> read(final ByteBuffer wkb) throws Exception {
        final GeometryWrapper<G> wrapper = geometryFactory.parseWKB(wkb);
        CoordinateReferenceSystem crs = defaultCRS;
        if (fromSridToCRS != null) {
            final OptionalInt srid = wrapper.getSRID();
            if (srid.isPresent()) {
                crs = fromSridToCRS.fetchCRS(srid.getAsInt());
            }
        }
        if (crs != null) {
            wrapper.setCoordinateReferenceSystem(crs);
        }
        return wrapper;
    }
}
