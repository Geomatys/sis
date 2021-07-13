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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.opengis.referencing.crs.CoordinateReferenceSystem;



/**
 * Not THREAD-SAFE !
 * Search for geometric information in specialized SQL for Simple feature tables (refer to
 * <a href="https://www.ogc.org/standards/sfs">OGC 06-104r4 (Simple feature access - Part 2: SQL option)</a>).
 *
 * @implNote <a href="https://www.jooq.org/doc/3.12/manual/sql-execution/fetching/pojos/#N5EFC1">I miss JOOQ...</a>
 *
 * @author Alexis Manin (Geomatys)
 * @version 2.0
 * @since   2.0
 * @module
 */
final class GeometryIdentification implements AutoCloseable {
    /**
     * A statement for fetching geometric information for a specific column.
     */
    private final PreparedStatement columnQuery;

    private final CachedStatements crsIdent;

    /**
     * Describes if geometry column registry include a column for geometry types, according that one can apparently omit
     * it (see Simple_feature_access_-_Part_2_SQL_option_v1.2.1, section 6.2: Architecture - SQL implementation using
     * Geometry Types).
     */
    private final boolean typeIncluded;

    public GeometryIdentification(final Session<?> session, Connection c) throws SQLException {
        this(session, c, "geometry_columns", "f_geometry_column", "geometry_type");
    }

    public GeometryIdentification(final Session<?> session, Connection c, String identificationTable, String geometryColumnName,
            String typeColumnName) throws SQLException
    {
        typeIncluded = typeColumnName != null;
        columnQuery = c.prepareStatement(
                "SELECT srid" + (typeIncluded ? ", "+typeColumnName : "") + ' ' +
                "FROM "+identificationTable+" " +
                "WHERE f_table_schema LIKE ? " +
                "AND f_table_name = ? " +
                "AND "+geometryColumnName+" = ?"
        );
        crsIdent = new CachedStatements(session, c);
    }

    void fetch(final Column target) throws Exception {
        if (target == null || target.origin == null) {
            return;
        }
        String schema = target.origin.schema;       // TODO: escape.
        if (schema == null) schema = "%";
        columnQuery.setString(1, schema);
        columnQuery.setString(2, target.origin.table);
        columnQuery.setString(3, target.name);
        try (ResultSet result = columnQuery.executeQuery()) {
            while (result.next()) {
                final int pgSrid = result.getInt(3);
                final String type = typeIncluded ? result.getString(4) : null;
                final CoordinateReferenceSystem crs = crsIdent.fetchCRS(pgSrid);
                target.setGeometry(type, crs, crsIdent.getLocale());
            }
        }
    }

    @Override
    public void close() throws Exception {
        try (AutoCloseable c1 = columnQuery;
             AutoCloseable c2 = crsIdent)
        {
            // Nothing to do, the intent is only to close all above resources.
        }
    }
}
