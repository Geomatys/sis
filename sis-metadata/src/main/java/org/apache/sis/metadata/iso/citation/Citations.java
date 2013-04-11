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
package org.apache.sis.metadata.iso.citation;

import org.opengis.metadata.citation.Citation;
import org.apache.sis.util.Static;
import org.apache.sis.xml.IdentifierSpace;
import org.apache.sis.internal.simple.SimpleCitation;
import org.apache.sis.util.CharSequences;


/**
 * A set of pre-defined constants and static methods working on {@linkplain Citation citations}.
 * The citation constants declared in this class are for:
 *
 * <ul>
 *   <li><cite>Organizations</cite> (e.g. {@linkplain #OGC})</li>
 *   <li><cite>Specifications</cite> (e.g. {@linkplain #WMS})</li>
 *   <li><cite>Authorities</cite> that maintain definitions of codes (e.g. {@linkplain #EPSG})</li>
 * </ul>
 *
 * In the later case, the citations are actually of kind {@link IdentifierSpace}.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @since   0.3 (derived from geotk-2.2)
 * @version 0.3
 * @module
 */
public final class Citations extends Static {
    /**
     * The <a href="http://www.iso.org/">International Organization for Standardization</a>.
     *
     * @category Organization
     */
    public static final Citation ISO = new SimpleCitation("ISO");

    /**
     * The <a href="http://www.opengeospatial.org">Open Geospatial Consortium</a> organization.
     * "Open Geospatial Consortium" is the new name for "OpenGIS consortium".
     *
     * @see org.apache.sis.io.wkt.Convention#OGC
     * @category Organization
     */
    public static final Citation OGC = new SimpleCitation("OGC");

    /**
     * <cite>International Standard Book Number</cite> (ISBN) defined by ISO-2108.
     * The ISO-19115 metadata standard defines a specific attribute for this information,
     * but the SIS library handles it like any other identifier.
     *
     * @see DefaultCitation#getISBN()
     *
     * @category Code space
     */
    public static final IdentifierSpace<String> ISBN = DefaultCitation.ISBN;

    /**
     * <cite>International Standard Serial Number</cite> (ISSN) defined by ISO-3297.
     * The ISO-19115 metadata standard defines a specific attribute for this information,
     * but the SIS library handles it like any other identifier.
     *
     * @see DefaultCitation#getISSN()
     *
     * @category Code space
     */
    public static final IdentifierSpace<String> ISSN = DefaultCitation.ISSN;

    /**
     * List of citations declared in this class.
     */
    private static final Citation[] AUTHORITIES = {
        ISO, OGC, ISBN, ISSN
    };

    /**
     * Do not allows instantiation of this class.
     */
    private Citations() {
    }

    /**
     * Returns a citation of the given name. The method makes the following choice:
     *
     * <ul>
     *   <li>If the given title is {@code null} or empty (ignoring spaces), then this method
     *       returns {@code null}.</li>
     *   <li>Otherwise if the given name matches a {@linkplain Citation#getTitle() title} or an
     *       {@linkplain Citation#getAlternateTitles() alternate titles} of one of the pre-defined
     *       constants ({@link #EPSG}, {@link #GEOTIFF}, <i>etc.</i>), then that constant
     *       is returned.</li>
     *   <li>Otherwise, a new citation is created with the specified name as the title.</li>
     * </ul>
     *
     * @param  title The citation title (or alternate title), or {@code null}.
     * @return A citation using the specified name, or {@code null} if the given title is null
     *         or empty.
     */
    public static Citation fromName(String title) {
        if (title == null || ((title = CharSequences.trimWhitespaces(title)).isEmpty())) {
            return null;
        }
        for (int i=0; i<AUTHORITIES.length; i++) {
            final Citation citation = AUTHORITIES[i];
            if (titleMatches(citation, title)) {
                return citation;
            }
        }
        return new SimpleCitation(title);
    }

    /**
     * Returns {@code true} if at least one {@linkplain Citation#getTitle() title} or
     * {@linkplain Citation#getAlternateTitles() alternate title} in {@code c1} is leniently
     * equal to a title or alternate title in {@code c2}. The comparison is case-insensitive
     * and ignores every character which is not a {@linkplain Character#isLetterOrDigit(int)
     * letter or a digit}. The titles ordering is not significant.
     *
     * @param  c1 The first citation to compare, or {@code null}.
     * @param  c2 the second citation to compare, or {@code null}.
     * @return {@code true} if both arguments are non-null, and at least one title or
     *         alternate title matches.
     */
    public static boolean titleMatches(final Citation c1, final Citation c2) {
        return org.apache.sis.internal.util.Citations.titleMatches(c1, c2);
    }

    /**
     * Returns {@code true} if the {@linkplain Citation#getTitle() title} or any
     * {@linkplain Citation#getAlternateTitles() alternate title} in the given citation
     * matches the given string. The comparison is case-insensitive and ignores every character
     * which is not a {@linkplain Character#isLetterOrDigit(int) letter or a digit}.
     *
     * @param  citation The citation to check for, or {@code null}.
     * @param  title The title or alternate title to compare, or {@code null}.
     * @return {@code true} if both arguments are non-null, and the title or alternate
     *         title matches the given string.
     */
    public static boolean titleMatches(final Citation citation, String title) {
        return org.apache.sis.internal.util.Citations.titleMatches(citation, title);
    }

    /**
     * Returns {@code true} if at least one {@linkplain Citation#getIdentifiers() identifier} in
     * {@code c1} is equal to an identifier in {@code c2}. The comparison is case-insensitive
     * and ignores every character which is not a {@linkplain Character#isLetterOrDigit(int)
     * letter or a digit}. The identifier ordering is not significant.
     *
     * <p>If (and <em>only</em> if) the citations do not contains any identifier, then this method
     * fallback on titles comparison using the {@link #titleMatches(Citation,Citation) titleMatches}
     * method. This fallback exists for compatibility with client codes using the citation
     * {@linkplain Citation#getTitle() titles} without identifiers.</p>
     *
     * @param  c1 The first citation to compare, or {@code null}.
     * @param  c2 the second citation to compare, or {@code null}.
     * @return {@code true} if both arguments are non-null, and at least one identifier,
     *         title or alternate title matches.
     */
    public static boolean identifierMatches(final Citation c1, final Citation c2) {
        return org.apache.sis.internal.util.Citations.identifierMatches(c1, c2);
    }

    /**
     * Returns {@code true} if any {@linkplain Citation#getIdentifiers() identifiers} in the given
     * citation matches the given string. The comparison is case-insensitive and ignores every
     * character which is not a {@linkplain Character#isLetterOrDigit(int) letter or a digit}.
     *
     * <p>If (and <em>only</em> if) the citation does not contain any identifier, then this method
     * fallback on titles comparison using the {@link #titleMatches(Citation,String) titleMatches}
     * method. This fallback exists for compatibility with client codes using citation
     * {@linkplain Citation#getTitle() titles} without identifiers.</p>
     *
     * @param  citation The citation to check for, or {@code null}.
     * @param  identifier The identifier to compare, or {@code null}.
     * @return {@code true} if both arguments are non-null, and the title or alternate title
     *         matches the given string.
     */
    public static boolean identifierMatches(final Citation citation, final String identifier) {
        return org.apache.sis.internal.util.Citations.identifierMatches(citation, identifier);
    }

    /**
     * Returns the shortest identifier for the specified citation, or the title if there is
     * no identifier. This method is useful for extracting the namespace from an authority,
     * for example {@code "EPSG"}.
     *
     * @param  citation The citation for which to get the identifier, or {@code null}.
     * @return The shortest identifier of the given citation, or {@code null} if the
     *         given citation was null or doesn't declare any identifier or title.
     */
    public static String getIdentifier(final Citation citation) {
        return org.apache.sis.internal.util.Citations.getIdentifier(citation);
    }
}
