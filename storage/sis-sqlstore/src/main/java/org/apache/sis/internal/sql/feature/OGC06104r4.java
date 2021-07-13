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
import java.util.Optional;

import org.opengis.referencing.crs.CoordinateReferenceSystem;

import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.util.collection.BackingStoreException;

/**
 * Geometric SQL mapping based on <a href="https://www.ogc.org/standards/sfs">OpenGIS® Implementation Standard
 * for Geographic information -Simple feature access -Part 2: SQL option</a>.
 *
 * @implNote WARNING: This class will (almost certainly) not work as is. It provides a base implementation for geometry
 * access on any SQL simple feature compliant database, but the standard does not specify precisely what mode of
 * representation is the default (WKB or WKT). The aim is to base specific drivers on this class (see {@link PostGISMapping}
 * for an example).
 *
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class OGC06104r4<G> extends Session<G> {

    final GeometryIdentification identifyGeometries;

    private OGC06104r4(final Geometries<G> geomLibrary, final StoreListeners listeners, final Connection c) throws SQLException {
        super(geomLibrary, false, listeners);
        this.identifyGeometries = new GeometryIdentification(this, c);
    }

    @Override
    public ValueGetter<?> getMapping(final Column columnDefinition) {
        if (columnDefinition.typeName != null && "geometry".equalsIgnoreCase(columnDefinition.typeName)) {
            // In case of a computed column, geometric definition could be null.
            try {
                identifyGeometries.fetch(columnDefinition);
            } catch (Exception e) {
                throw new BackingStoreException(e); // TODO
            }
            String geometryType = columnDefinition.getGeometryType();
            final Class<?> geomClass = getGeometricClass(geometryType, geomLibrary);
            return new WKBReader(geomClass, columnDefinition.getGeometryCRS());
        }
        return null;
    }

    public void close() throws Exception {
        identifyGeometries.close();
    }

    static Class<?> getGeometricClass(String geometryType, final Geometries<?> library) {
        if (geometryType == null) return library.rootClass;

        // remove Z, M or ZM suffix
        if (geometryType.endsWith("M")) geometryType = geometryType.substring(0, geometryType.length()-1);
        if (geometryType.endsWith("Z")) geometryType = geometryType.substring(0, geometryType.length()-1);

        final Class<?> geomClass;
        switch (geometryType) {
            case "POINT":
                geomClass = library.pointClass;
                break;
            case "LINESTRING":
                geomClass = library.polylineClass;
                break;
            case "POLYGON":
                geomClass = library.polygonClass;
                break;
            default: geomClass = library.rootClass;
        }
        return geomClass;
    }

    private final class WKBReader extends ValueGetter<Object> {

        final CoordinateReferenceSystem crsToApply;

        private WKBReader(Class<?> geomClass, CoordinateReferenceSystem crsToApply) {
            super(geomClass);
            this.crsToApply = crsToApply;
        }

        @Override
        public Object getValue(final ResultSet source, final int columnIndex) throws Exception {
            final byte[] bytes = source.getBytes(columnIndex);
            if (bytes == null) return null;
            /*
             * TODO: it is not sure that database driver return WKB, so we should
             * find a way to ensure that SQL queries use `ST_AsBinary` function.
             */
            return new EWKBReader<>(geomLibrary, crsToApply).read(bytes).implementation();
        }

        @Override
        public Optional<CoordinateReferenceSystem> getCRS() {
            return Optional.ofNullable(crsToApply);
        }
    }

    public static final class Spi implements Session.Spi {
        @Override
        public Session<?> create(final GeometryLibrary geomlib, final StoreListeners listeners, Connection c) throws SQLException {
            return new OGC06104r4<>(Geometries.implementation(geomlib), listeners, c);
        }
    }
}
