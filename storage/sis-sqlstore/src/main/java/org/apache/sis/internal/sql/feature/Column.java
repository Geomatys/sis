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

import java.util.Locale;
import java.sql.Types;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.apache.sis.internal.metadata.sql.Reflection;
import org.apache.sis.internal.metadata.sql.SQLBuilder;
import org.apache.sis.internal.metadata.sql.SQLUtilities;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.util.resources.Errors;


/**
 * Information (name, data type…) about a table column. It contains information extracted from
 * {@linkplain DatabaseMetaData#getColumns(String, String, String, String) database metadata},
 * possibly completed with information about a geometry column.
 * The aim is to describe all information about a column that is needed for mapping to feature model.
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 *
 * @see ResultSet#getMetaData()
 * @see DatabaseMetaData#getColumns(String, String, String, String)
 *
 * @since 1.1
 * @module
 */
final class Column {
    /**
     * The table that contains this column, or {@code null} if unknown.
     * It may be null when this column is created from query analysis.
     */
    final TableReference origin;

    /**
     * Name of the column.
     *
     * @see Reflection#COLUMN_NAME
     */
    final String name;

    /**
     * Title to use for displays. This is the name specified by the {@code AS} keyword in a {@code SELECT} clause.
     * This is never null but may be identical to {@link #name} if no label was specified.
     */
    final String label;

    /**
     * Type of values as one of the constant enumerated in {@link Types} class.
     *
     * @see Reflection#DATA_TYPE
     */
    final int type;

    /**
     * A name for the value type, free-text from the database engine. For more information about this, please see
     * {@link DatabaseMetaData#getColumns(String, String, String, String)} and {@link Reflection#TYPE_NAME}.
     *
     * @see Reflection#TYPE_NAME
     */
    final String typeName;

    /**
     * The column size, or 0 if not applicable. For texts, this is the maximum number of characters allowed.
     * For numbers, this is the maximum number of digits. For blobs, this is a limit in number of bytes.
     *
     * @see Reflection#COLUMN_SIZE
     * @see ResultSetMetaData#getPrecision(int)
     */
    final int precision;

    /**
     * Whether the column can have null values.
     *
     * @see Reflection#IS_NULLABLE
     */
    final boolean isNullable;

    /**
     * If this column is a geometry column, the type of the geometry objects. Otherwise {@code null}.
     *
     * @todo Merge with {@link #type}? Or use {@link org.apache.sis.internal.feature.GeometryType}?
     */
    private String geometryType;

    /**
     * If this column is a geometry column, the Coordinate Reference System (CRS). Otherwise {@code null}.
     * This is determined from the geometry Spatial Reference Identifier (SRID).
     */
    private CoordinateReferenceSystem geometryCRS;

    /**
     * Creates a new column from database metadata.
     * Information are fetched from current {@code ResultSet} row.
     * This method does not change cursor position.
     *
     * @param  analyzer  the analyzer which is creating this column.
     * @param  origin    the table that contains this column.
     * @param  metadata  the
     * @throws SQLException if an error occurred while fetching metadata.
     *
     * @see DatabaseMetaData#getColumns(String, String, String, String)
     */
    Column(final Analyzer analyzer, final TableReference origin, final ResultSet metadata) throws SQLException {
        this.origin  = origin;
        label = name = analyzer.getUniqueString(metadata, Reflection.COLUMN_NAME);
        type         = metadata.getInt(Reflection.DATA_TYPE);
        typeName     = metadata.getString(Reflection.TYPE_NAME);
        precision    = metadata.getInt(Reflection.COLUMN_SIZE);
        isNullable   = Boolean.TRUE.equals(SQLUtilities.parseBoolean(metadata.getString(Reflection.IS_NULLABLE)));
    }

    /**
     * Creates a new column from the result of a query.
     *
     * @param  origin    the table that contains this column.
     * @param  metadata  value of {@link ResultSet#getMetaData()}.
     * @throws SQLException if an error occurred while fetching metadata.
     *
     * @see ResultSet#getMetaData()
     */
    Column(final TableReference origin, final ResultSetMetaData metadata, final int column) throws SQLException {
        this.origin = origin;
        name        = metadata.getColumnName(column);
        label       = metadata.getColumnLabel(column);
        type        = metadata.getColumnType(column);
        typeName    = metadata.getColumnTypeName(column);
        precision   = metadata.getPrecision(column);
        isNullable  = metadata.isNullable(column) == ResultSetMetaData.columnNullable;
    }

    /**
     * Modifies this column for declaring it as a geometry column.
     *
     * @param  type    the type of geometry objects.
     * @param  crs     the Coordinate Reference System (CRS), or {@code null} if unknown.
     * @param  locale  locale for error message, if any.
     */
    void setGeometry(final String type, final CoordinateReferenceSystem crs, final Locale locale)
            throws DataStoreContentException
    {
        final String property;
        if (geometryType != null && !geometryType.equals(type)) {
            property = "geometryType";
        } else if (geometryCRS != null && !geometryCRS.equals(crs)) {
            property = "geometryCRS";
        } else {
            geometryType = type;
            geometryCRS = crs;
            return;
        }
        throw new DataStoreContentException(Errors.getResources(locale)
                .getString(Errors.Keys.ValueAlreadyDefined_1, property));
    }

    /**
     * If this column is a geometry column, returns the type of the geometry objects.
     * Otherwise returns {@code null}.
     */
    final String getGeometryType() {
        return geometryType;
    }

    /**
     * If this column is a geometry column, returns the coordinate reference system.
     * Otherwise returns {@code null}. The CRS may also be null even for a geometry column if it is unspecified.
     */
    final CoordinateReferenceSystem getGeometryCRS() {
        return geometryCRS;
    }

    /**
     * Appends the name and label of this column in a SQL statement.
     *
     * @param  target  builder of SQL statement where to append this column name.
     * @return {@code target} for invocation chaining.
     */
    SQLBuilder append(final SQLBuilder target) {
        target.appendIdentifier(name);
        if (!name.equals(label)) {
            target.append(" AS ").appendIdentifier(label);
        }
        return target;
    }
}
