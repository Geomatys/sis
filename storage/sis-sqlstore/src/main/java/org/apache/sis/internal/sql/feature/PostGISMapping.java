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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.internal.system.Modules;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.util.collection.BackingStoreException;
import org.apache.sis.util.logging.Logging;

import static org.apache.sis.internal.sql.feature.OGC06104r4.getGeometricClass;
import org.apache.sis.storage.event.StoreListeners;

/**
 * Maps geometric values between PostGIS natural representation (Hexadecimal EWKT) and SIS.
 * For more information about EWKB format, see:
 * <ul>
 *     <li><a href="http://postgis.refractions.net/documentation/manual-1.3/ch04.html#id2571020">PostGIS manual, section 4.1.2</a></li>
 *     <li><a href="https://www.ibm.com/support/knowledgecenter/SSGU8G_14.1.0/com.ibm.spatial.doc/ids_spat_285.htm">IBM WKB description</a></li>
 * </ul>
 *
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public final class PostGISMapping<G> extends Session<G> {
    /**
     * Pattern that search for a semantic version with optional minor version (or patch).
     * PostGIS version query returns a complex text starting with its numerical version.
     * In the future, we could also parse other information in the version text.
     * Example of a PostGIS version string: {@code 3.1 USE_GEOS=1 USE_PROJ=1 USE_STATS=1}
     */
    static final Pattern POSTGIS_VERSION_PARSER = Pattern.compile("^(\\d+)(\\.\\d+)*(?:\\s+|$)");

    final GeometryIdentification identifyGeometries;
    final GeometryIdentification identifyGeographies;

    private PostGISMapping(final Geometries<G> geomLibrary, final StoreListeners listeners, Connection c) throws SQLException {
        super(geomLibrary, false, listeners);
        this.identifyGeometries = new GeometryIdentification(this, c, "geometry_columns", "f_geometry_column", "type");
        this.identifyGeographies = new GeometryIdentification(this, c, "geography_columns", "f_geography_column", "type");
    }

    @Override
    public ValueGetter<?> getMapping(final Column definition) {
        if (definition.typeName == null) return null;
        switch (definition.typeName.trim().toLowerCase()) {
            case "geometry":
                return forGeometry(definition, identifyGeometries);
            case "geography":
                return forGeometry(definition, identifyGeographies);
            default: return null;
        }
    }

    private ValueGetter<?> forGeometry(Column definition, GeometryIdentification ident) {
        // In case of a computed column, geometric definition could be null.
        try {
            ident.fetch(definition);
        } catch (Exception e) {
            throw new BackingStoreException(e);     // TODO
        }
        String geometryType = definition.getGeometryType();
        final Class<?> geomClass = getGeometricClass(geometryType, geomLibrary);

        CoordinateReferenceSystem crs = definition.getGeometryCRS();
        if (crs == null) {
            return new HexEWKB(geomClass);
        } else {
            // TODO: activate optimisation : WKB is lighter, but we need to modify user query, and to know CRS in advance.
            //geometryDecoder = new WKBReader(geomDef.crs);
            return new HexEWKB(geomClass, crs);
        }
    }

    public void close() throws Exception {
        identifyGeometries.close();
    }

    public static final class Spi implements Session.Spi {
        @Override
        public Session<?> create(final GeometryLibrary geomlib, final StoreListeners listeners, Connection c) throws SQLException {
            try (Statement st = c.createStatement();
                 ResultSet result = st.executeQuery("SELECT PostGIS_version();"))
            {
                if (result.next()) {
                    final String rawVersion = result.getString(1);
                    if (!verifyVersion(rawVersion, 2)) {
                        throw new SQLException("Incompatible PostGIS version. At least version 2 is required, but database declares: " + rawVersion);
                    }
                } else {
                    // TODO
                }
            } catch (SQLException e) {
                /*
                 * TODO: need to handle the case where PostGIS is not installed.
                 */
                final Logger logger = Logging.getLogger(Modules.SQL);
                logger.warning("No compatible PostGIS version found. Binding deactivated. See debug logs for more information");
                logger.log(Level.FINE, "Cannot determine PostGIS version", e);
                return null;
            }
            return new PostGISMapping<>(Geometries.implementation(geomlib), listeners, c);
        }
    }

    /**
     * Ensure that given string starts with a valid semantic version, and that its major number is equal or superior to
     * given value.
     *
     * @param versionString The text starting with a semantic version.
     * @param minAllowed Lowest acceptable value for major version number.
     * @return True if both following conditions match:
     * <ul>
     *     <li>Input text is considered a valid version string.</li>
     *     <li>Major version number is same or higher than given minimum.</li>
     * </ul>
     * @see #POSTGIS_VERSION_PARSER used version regex
     */
    static boolean verifyVersion(String versionString, int minAllowed) {
        final Matcher versionMatcher = POSTGIS_VERSION_PARSER.matcher(versionString);
        if (!versionMatcher.find()) return false;

        final String majorVersionStr = versionMatcher.group(1);
        final int majorVersion = Integer.parseInt(majorVersionStr);
        return (majorVersion >= minAllowed);
    }

    private final class HexEWKB extends ValueGetter<Object> {

        private EWKBReader<?> reader;

        public HexEWKB(final Class<?> geomClass) {
            super(geomClass);
        }

        public HexEWKB(final Class<?> geomClass, final CoordinateReferenceSystem crsToApply) {
            super(geomClass);
            reader = new EWKBReader<>(geomLibrary, crsToApply);
        }

        @Override
        public Optional<CoordinateReferenceSystem> getCRS() {
            return Optional.ofNullable(reader.defaultCRS);
        }

        @Override
        public Object getValue(final ResultSet source, final int columnIndex) throws Exception {
            if (reader == null) {
                // TODO: this component is not properly closed. As connection closing should also close this component
                // statement, it should be Ok.However, a proper management would be better.
                final CachedStatements crsIdent;
                try {
                    crsIdent = new CachedStatements(PostGISMapping.this, source.getStatement().getConnection());
                } catch (SQLException e) {
                    throw new BackingStoreException(e);
                }
                reader = new EWKBReader<>(geomLibrary, null);
                reader.setSridResolver(crsIdent);
            }
            final String hexa = source.getString(columnIndex);
            return hexa == null ? null : reader.readHexa(hexa).implementation();
        }
    }
}
