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
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.text.ParseException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.internal.referencing.DefinitionVerifier;
import org.apache.sis.internal.referencing.ReferencingUtilities;
import org.apache.sis.internal.system.Modules;
import org.apache.sis.io.wkt.Convention;
import org.apache.sis.io.wkt.WKTFormat;
import org.apache.sis.io.wkt.Warnings;
import org.apache.sis.referencing.CRS;
import org.apache.sis.util.Localized;


/**
 * A set of prepared statements to create when first needed and to reuse as long as the connection is in scope.
 * The prepared statement tasks include:
 *
 * <ul>
 *   <li>Fetching a Coordinate Reference System (CRS) from a SRID.</li>
 * </ul>
 *
 * This class is <strong>not</strong> thread-safe. Each instance should be used in a single thread.
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class CachedStatements implements Localized, AutoCloseable {
    /**
     * The table containing CRS definitions, as specified by ISO 19125 / OGC Simple feature access part 2.
     * Note that the standard specifies table names in upper-case letters, which is also the default case
     * specified by the SQL standard. However some databases use lower cases instead. This table name can
     * be used unquoted for letting the database engine converts the case.
     *
     * @todo Provide also a non-static field with the case used by database engine, as computed by {@link Analyzer}.
     */
    static final String SPATIAL_REF_SYS = "SPATIAL_REF_SYS";

    /**
     * The session that created this set of cached statements. This object includes the
     * cache of CRS created from SRID codes and the listeners where to send warnings.
     * A session does <strong>not</strong> contain a JDBC {@link Connection}.
     */
    private final Session<?> session;

    /**
     * Connection to use for creating the prepared statements.
     * This connection will <strong>not</strong> be closed by this class.
     */
    private final Connection connection;

    /**
     * The statement for fetching CRS Well-Known Text (WKT) from a SRID code.
     *
     * @see <a href="http://postgis.refractions.net/documentation/manual-1.3/ch04.html#id2571265">PostGIS documentation</a>
     */
    private PreparedStatement wktFromSrid;

    /**
     * The object to use for parsing Well-Known Text (WKT), created when first needed.
     */
    private WKTFormat wktReader;

    /**
     * Creates an initially empty {@code CachedStatements} which will use
     * the given connection for creating {@link PreparedStatement}s.
     */
    CachedStatements(final Session<?> session, final Connection connection) {
        this.session    = session;
        this.connection = connection;
    }

    /**
     * Returns the locale used for warnings and error messages.
     */
    @Override
    public Locale getLocale() {
        return session.listeners.getLocale();
    }

    /**
     * Gets a Coordinate Reference System for to given SRID.
     * If the given SRID is zero or negative, then this method returns {@code null}.
     * Otherwise the CRS is decoded from the database {@value #SPATIAL_REF_SYS} table.
     *
     * @param  srid  the Spatial Reference Identifier (SRID) to resolve as a CRS object.
     * @return the CRS associated to the given SRID, or {@code null} if the SRID is zero.
     * @throws DataStoreContentException if the CRS can not be fetched. Possible reasons are:
     *         no entry found in the {@value #SPATIAL_REF_SYS} table, or more than one entry is found,
     *         or a single entry exists but has no WKT definition and its authority code is unsupported by SIS.
     * @throws ParseException if the WKT can not be parsed.
     * @throws SQLException if a SQL error occurred.
     */
    CoordinateReferenceSystem fetchCRS(final int srid) throws Exception {
        /*
         * In PostGIS 1, srid value -1 was used for "unknown CRS".
         * Since PostGIS 2, srid value for unknown CRS became 0.
         */
        if (srid <= 0) return null;
        return session.cacheOfCRS.getOrCreate(srid, () -> parseCRS(srid));
    }

    /**
     * Invoked when the requested CRS is not in the cache. This method gets the entry from the
     * {@value #SPATIAL_REF_SYS} table then gets the CRS from its authority code if possible,
     * or fallback on the WKT otherwise.
     *
     * @param  srid  the Spatial Reference Identifier (SRID) of the CRS to create from the database content.
     * @return the CRS created from database content.
     * @throws Exception if an SQL error, parsing error or other error occurred.
     */
    private CoordinateReferenceSystem parseCRS(final int srid) throws Exception {
        if (wktFromSrid == null) {
            wktFromSrid = connection.prepareStatement(
                    "SELECT auth_name, auth_srid, srtext FROM " + SPATIAL_REF_SYS + " WHERE srid=?");
        }
        wktFromSrid.setInt(1, srid);
        CoordinateReferenceSystem crs = null;
        NoSuchAuthorityCodeException authorityError = null;
        LogRecord warning = null;
        try (ResultSet result = wktFromSrid.executeQuery()) {
            while (result.next()) {
                /*
                 * If the authority code is recognized, use that code instead of WKT definition
                 * because the EPSG database (for example) contains more information than WKT.
                 */
                CoordinateReferenceSystem fromAuthority = null;
                final String authority = result.getString(1);
                if (authority != null && !authority.isEmpty()) {
                    final int code = result.getInt(2);
                    if (!result.wasNull()) try {
                        final CRSAuthorityFactory factory = CRS.getAuthorityFactory(authority);
                        fromAuthority = factory.createCoordinateReferenceSystem(Integer.toString(code));
                    } catch (NoSuchAuthorityCodeException e) {      // Include NoSuchAuthorityFactoryException.
                        authorityError = e;
                    }
                }
                /*
                 * Parse the WKT unconditionally, even if we already got the CRS from authority code.
                 * It the later case, the CRS from WKT will be used only for a consistency check and
                 * the main CRS will be the one from authority.
                 */
                CoordinateReferenceSystem fromWKT = null;
                final String wkt = result.getString(3);
                if (wkt != null && !wkt.isEmpty()) {
                    if (wktReader == null) {
                        wktReader = new WKTFormat(null, null);
                        wktReader.setConvention(Convention.WKT1_COMMON_UNITS);
                    }
                    final Object parsed;
                    try {
                        parsed = wktReader.parseObject(wkt);
                    } catch (ParseException e) {
                        if (authorityError != null) {
                            e.addSuppressed(authorityError);
                        }
                        throw e;
                    }
                    if (parsed instanceof CoordinateReferenceSystem) {
                        fromWKT = (CoordinateReferenceSystem) parsed;
                    } else {
                        throw invalidSRID(Resources.Keys.UnexpectedTypeForSRID_2,
                                ReferencingUtilities.getInterface(parsed), srid, authorityError);
                    }
                }
                /*
                 * If one of the CRS is null, take the non-null one. If both CRSs are defined (which is the usual case),
                 * verify that they are consistent. Inconsistency will be logged as warning if the rest of the operation
                 * succeed.
                 */
                final DefinitionVerifier v = DefinitionVerifier.compare(fromWKT, fromAuthority, getLocale());
                if (v.recommendation != null) {
                    if (crs == null) {
                        crs = v.recommendation;
                    } else if (!crs.equals(v.recommendation)) {
                        throw invalidSRID(Resources.Keys.DuplicatedSRID_2, SPATIAL_REF_SYS, srid, authorityError);
                    }
                    warning = v.warning(false);
                    if (warning == null && fromWKT != null) {
                        /*
                         * Following warnings may have occurred during WKT parsing and are considered minor.
                         * They will be reported only if there is no more important warnings to report.
                         */
                        final Warnings w = wktReader.getWarnings();
                        if (w != null) {
                            warning = new LogRecord(Level.WARNING, w.toString(getLocale()));
                        }
                    }
                }
            }
        }
        /*
         * Finished to parse entries from the "spatial_ref_sys" table.
         * Reports warning if any, then return the non-null CRS.
         */
        wktFromSrid.clearParameters();
        if (crs == null) {
            if (authorityError != null) {
                throw authorityError;
            }
            throw invalidSRID(Resources.Keys.UnknownSRID_2, SPATIAL_REF_SYS, srid, null);
        }
        if (warning != null) {
            warning.setLoggerName(Modules.SQL);
            warning.setSourceClassName(getClass().getName());
            warning.setSourceMethodName("fetchCRS");
            session.listeners.warning(warning);
        }
        return crs;
    }

    /**
     * Creates the exception to throw for an invalid SRID. The message is expected to have two arguments,
     * {@code complement} and {@code srid} if that order, where the "complement" can be a table name or a
     * class name depending on the message.
     *
     * @param  message     key of the message to create.
     * @param  complement  first argument in message formatting.
     * @param  srid        second argument in message formatting.
     * @param  suppressed  exception to add as a suppressed exception.
     * @return the exception to throw.
     */
    private DataStoreContentException invalidSRID(final short message, final Object complement, final int srid,
            final NoSuchAuthorityCodeException suppressed)
    {
        final DataStoreContentException e = new DataStoreContentException(
                Resources.forLocale(getLocale()).getString(message, complement, srid));
        if (suppressed != null) {
            e.addSuppressed(suppressed);
        }
        return e;
    }

    /**
     * Closes all prepared statements. This method does <strong>not</strong> close the connection.
     */
    @Override
    public void close() throws SQLException {
        if (wktFromSrid != null) {
            wktFromSrid.close();
            wktFromSrid = null;
        }
    }
}
