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
package org.apache.sis.internal.sql.postgis;

import java.util.Set;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import java.util.logging.Level;
import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.internal.sql.feature.InfoStatements;
import org.apache.sis.internal.sql.feature.Column;
import org.apache.sis.internal.sql.feature.Database;
import org.apache.sis.internal.sql.feature.ValueGetter;
import org.apache.sis.internal.sql.feature.Resources;
import org.apache.sis.internal.sql.feature.SelectionClauseWriter;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.util.Version;


/**
 * Information about a connection to a PostgreSQL + PostGIS database.
 * This class specializes some of the functions for converting PostGIS objects to Java objects.
 * The PostGIS database is optional; it is possible to use PostgreSQL alone as a store of features without geometries.
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public final class Postgres<G> extends Database<G> {
    /**
     * Version of PostGIS extension, or {@code null} if PostGIS has not been found.
     */
    private final Version postgisVersion;

    /**
     * Creates a new session for a PostGIS database.
     *
     * @param  source       provider of (pooled) connections to the database.
     * @param  connection   the connection to the database. Should be considered as read-only.
     * @param  metadata     metadata about the database for which a session is created.
     * @param  geomLibrary  the factory to use for creating geometric objects.
     * @param  listeners    where to send warnings.
     * @throws SQLException if an error occurred while reading database metadata.
     */
    public Postgres(final DataSource source, final Connection connection, final DatabaseMetaData metadata,
                    final Geometries<G> geomLibrary, final StoreListeners listeners) throws SQLException
    {
        super(source, metadata, geomLibrary, listeners);
        Version version = null;
        try (Statement st = connection.createStatement();
             ResultSet result = st.executeQuery("SELECT public.PostGIS_version();"))
        {
            while (result.next()) {
                version = parseVersion(result.getString(1));
                if (version != null) break;
            }
        } catch (SQLException e) {
            log(Resources.forLocale(null).getLogRecord(Level.CONFIG,
                Resources.Keys.SpatialExtensionNotFound_1, "PostGIS"));
        }
        postgisVersion = version;
    }

    /**
     * Returns the version number of PostGIS extension.
     * PostGIS version query returns a detailed text starting with its numerical version.
     * Example of a PostGIS version string: {@code 3.1 USE_GEOS=1 USE_PROJ=1 USE_STATS=1}.
     * The version is assumed at the beginning of the string and terminated by a space.
     * In the future, we could also parse other information in the version text.
     *
     * @param  version  the text starting with a semantic version, or {@code null}.
     * @return major version number, or {@code null} if can not be parsed.
     */
    static Version parseVersion(String version) {
        if (version != null) {
            final int s = version.indexOf(' ');
            if (s >= 0) {
                version = version.substring(0, s);
            }
            if (!version.isEmpty()) {
                return new Version(version);
            }
        }
        return null;
    }

    /**
     * Returns a function for getting values from a column having the given definition.
     * The given definition should include data SQL type and type name.
     * If no match is found, then this method returns {@code null}.
     */
    @Override
    public ValueGetter<?> getMapping(final Column columnDefinition) {
        if ("geography".equalsIgnoreCase(columnDefinition.typeName)) {
            return forGeometry(columnDefinition);
        }
        return super.getMapping(columnDefinition);
    }

    /**
     * Prepares a cache of statements about spatial information using the given connection.
     * Statements will be created only when first needed.
     *
     * @param  connection  the connection to use for creating statements.
     * @return a cache of prepared statements about spatial information.
     */
    @Override
    protected InfoStatements createInfoStatements(final Connection connection) {
        return new ExtendedInfo(this, connection);
    }

    /**
     * Adds to the given set a list of tables to ignore when searching for feature tables.
     *
     * @param  ignoredTables  where to add names of tables to ignore.
     */
    @Override
    protected void addIgnoredTables(final Set<String> ignoredTables) {
        ignoredTables.add("geography_columns");     // Postgis 1+
        ignoredTables.add("raster_columns");        // Postgis 2
        ignoredTables.add("raster_overviews");
    }

    /**
     * Returns the converter from filters/expressions to the {@code WHERE} part of SQL statement.
     */
    @Override
    protected SelectionClauseWriter getFilterToSQL() {
        return ExtendedClauseWriter.INSTANCE;
    }
}