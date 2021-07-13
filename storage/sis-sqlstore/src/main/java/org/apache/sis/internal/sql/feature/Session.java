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

import java.sql.Types;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.util.collection.Cache;


/**
 * Information about a connection to a spatial database.
 * This class provides functions for converting objects between the types used in the Java language and the types or
 * SQL expressions expected by JDBC. Conversions may be straightforward (e.g. invoke {@link ResultSet#getInt(int)}
 * and wraps the result in an {@link Integer} if it {@linkplain ResultSet#wasNull() was not null}) or can be a more
 * complex process (e.g. decode a geometry Well-Known Binary). This class does not perform the conversions directly,
 * but instead provides a converter object for each specified SQL type.
 *
 * <p>This base class provides mapping for common types (text, numbers, temporal objects, <i>etc.</i>)
 * and for geometry types as specified in <a href="https://www.ogc.org/standards/sfs">OpenGIS®
 * Implementation Standard for Geographic information — Simple feature access — Part 2: SQL option</a>.
 * Subclasses can override some functions if a particular database software (e.g. PostGIS) provides
 * specialized methods or have non-standard behavior for some data types.</p>
 *
 * <h2>Multi-threading</h2>
 * This class is safe for concurrent use by many threads. This class does not hold JDBC resources such as
 * {@link Connection}. Those resources are created temporarily when needed by {@link CachedStatements}.
 *
 * @param  <G>  the type of geometry objects. Depends on the backing implementation (ESRI, JTS, Java2D…).
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public class Session<G> {
    /**
     * The factory to use for creating geometric objects.
     * For example the geometry implementations may be ESRI, JTS or Java2D objects.
     */
    final Geometries<G> geomLibrary;

    /**
     * Whether {@link Types#TINYINT} is a signed integer. Both conventions (-128 … 127 range and 0 … 255 range)
     * are found on the web. If unspecified, we conservatively assume unsigned bytes.
     * All other integer types are presumed signed.
     */
    private final boolean isByteSigned;

    /**
     * Where to send warnings.
     */
    protected final StoreListeners listeners;

    /**
     * Cache of Coordinate Reference Systems created for a given SRID.
     * SRID are primary keys in the {@value CachedStatements#SPATIAL_REF_SYS} table.
     * They are not EPSG codes, even if the numerical values are often the same.
     *
     * <p>This mapping depend on the content of {@value CachedStatements#SPATIAL_REF_SYS} table.
     * For that reason, a distinct cache exists for each database.</p>
     */
    final Cache<Integer, CoordinateReferenceSystem> cacheOfCRS;

    /**
     * Creates a new set of converters.
     *
     * @param  geomLibrary   the factory to use for creating geometric objects.
     * @param  isByteSigned  whether {@link Types#TINYINT} is a signed integer.
     * @param  listeners     where to send warnings.
     */
    protected Session(final Geometries<G> geomLibrary, final boolean isByteSigned, final StoreListeners listeners) {
        this.geomLibrary  = geomLibrary;
        this.isByteSigned = isByteSigned;
        this.listeners    = listeners;
        this.cacheOfCRS   = new Cache<>(7, 2, false);
    }

    /**
     * Returns a function for getting values from a column having the given definition.
     * The definition includes data SQL type and type name.
     * If no match is found, then this method returns {@code null}.
     *
     * <p>The default implementation handles types declared in the {@link Types} class
     * and the geometry types defined in the spatial extensions defined by OGC standard.
     * Subclasses should override if some types need to be handle in a non-standard way
     * for a particular database product.</p>
     *
     * @param  columnDefinition  information about the column to extract values from and expose through Java API.
     * @return converter to the corresponding java type, or {@code null} if this class can not find a mapping,
     */
    @SuppressWarnings("fallthrough")
    public ValueGetter<?> getMapping(final Column columnDefinition) {
        switch (columnDefinition.type) {
            case Types.BIT:
            case Types.BOOLEAN:                   return ValueGetter.AsBoolean.INSTANCE;
            case Types.TINYINT: if (isByteSigned) return ValueGetter.AsByte.INSTANCE;       // else fallthrough.
            case Types.SMALLINT:                  return ValueGetter.AsShort.INSTANCE;
            case Types.INTEGER:                   return ValueGetter.AsInteger.INSTANCE;
            case Types.BIGINT:                    return ValueGetter.AsLong.INSTANCE;
            case Types.REAL:                      return ValueGetter.AsFloat.INSTANCE;
            case Types.FLOAT:                     // Despite the name, this is implemented as DOUBLE in major databases.
            case Types.DOUBLE:                    return ValueGetter.AsDouble.INSTANCE;
            case Types.NUMERIC:                   // Similar to DECIMAL except that it uses exactly the specified precision.
            case Types.DECIMAL:                   return ValueGetter.AsBigDecimal.INSTANCE;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:               return ValueGetter.AsString.INSTANCE;
            case Types.DATE:                      return ValueGetter.AsDate.INSTANCE;
            case Types.TIME:                      return ValueGetter.AsLocalTime.INSTANCE;
            case Types.TIMESTAMP:                 return ValueGetter.AsInstant.INSTANCE;
            case Types.TIME_WITH_TIMEZONE:        return ValueGetter.AsOffsetTime.INSTANCE;
            case Types.TIMESTAMP_WITH_TIMEZONE:   return ValueGetter.AsOffsetDateTime.INSTANCE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:             return ValueGetter.AsBytes.INSTANCE;
            case Types.ARRAY:                     // TODO
            case Types.OTHER:
            case Types.JAVA_OBJECT:               return ValueGetter.AsObject.INSTANCE;
            default:                              return null;
        }
    }

    /**
     * Provider of {@link Session} instance.
     */
    public interface Spi {
        /**
         * Checks if database is compliant with this service specification, and create a mapper in such case.
         *
         * @param  geomlib     the library to use if the given mapping needs to create geometries. It mainly serves
         *                     to obtain a geometry factory through {@link Geometries#implementation(GeometryLibrary)}.
         * @param  listeners   where to send warnings.
         * @param  connection  the connection to the database. Should be considered as read-only.
         * @return type converters compatible with the database at given connection,
         *         or {@code null} if the database is not supported by this provider.
         * @throws SQLException if an error occurs while fetching information from database.
         */
        Session<?> create(final GeometryLibrary geomlib, final StoreListeners listeners, final Connection connection) throws SQLException;
    }
}
