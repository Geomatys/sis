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

import java.util.Date;
import java.util.Collection;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.opengis.metadata.Identifier;
import org.opengis.metadata.citation.Citation;
import org.opengis.metadata.citation.CitationDate;
import org.opengis.metadata.citation.PresentationForm;
import org.opengis.metadata.citation.ResponsibleParty;
import org.opengis.metadata.citation.Series;
import org.opengis.util.InternationalString;
import org.apache.sis.util.iso.Types;
import org.apache.sis.util.iso.SimpleInternationalString;
import org.apache.sis.util.collection.CollectionsExt;
import org.apache.sis.internal.jaxb.NonMarshalledAuthority;
import org.apache.sis.metadata.iso.ISOMetadata;
import org.apache.sis.xml.IdentifierSpace;

import static org.apache.sis.internal.jaxb.MarshalContext.filterIdentifiers;
import static org.apache.sis.internal.metadata.MetadataUtilities.toDate;
import static org.apache.sis.internal.metadata.MetadataUtilities.toMilliseconds;


/**
 * Standardized resource reference.
 *
 * {@section Unified identifiers view}
 * The ISO 19115 model provides specific attributes for the {@linkplain #getISBN() ISBN} and
 * {@linkplain #getISSN() ISSN} codes. However the SIS library handles those codes like any
 * other identifiers. Consequently the ISBN and ISSN codes are included in the collection
 * returned by {@link #getIdentifiers()}, except at XML marshalling time (for ISO 19139 compliance).
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @author  Cédric Briançon (Geomatys)
 * @since   0.3 (derived from geotk-2.1)
 * @version 0.3
 * @module
 */
@XmlType(name = "CI_Citation_Type", propOrder = {
    "title",
    "alternateTitles",
    "dates",
    "edition",
    "editionDate",
    "identifiers",
    "citedResponsibleParties",
    "presentationForms",
    "series",
    "otherCitationDetails",
    "collectiveTitle",
    "ISBN",
    "ISSN"
})
@XmlRootElement(name = "CI_Citation")
public class DefaultCitation extends ISOMetadata implements Citation {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 3490090845236158848L;

    /**
     * The authority for International Standard Book Number.
     *
     * <p><b>Implementation note:</b> This field is read by reflection in
     * {@link org.apache.sis.internal.jaxb.NonMarshalledAuthority}. IF this
     * field is renamed or moved, then {@code NonMarshalledAuthority} needs
     * to be updated.</p>
     */
    static final IdentifierSpace<String> ISBN = new NonMarshalledAuthority<>("ISBN", NonMarshalledAuthority.ISBN);

    /**
     * The authority for International Standard Serial Number.
     *
     * <p><b>Implementation note:</b> This field is read by reflection in
     * {@link org.apache.sis.internal.jaxb.NonMarshalledAuthority}. IF this
     * field is renamed or moved, then {@code NonMarshalledAuthority} needs
     * to be updated.</p>
     */
    static final IdentifierSpace<String> ISSN = new NonMarshalledAuthority<>("ISSN", NonMarshalledAuthority.ISSN);

    /**
     * Name by which the cited resource is known.
     */
    private InternationalString title;

    /**
     * Short name or other language name by which the cited information is known.
     * Example: "DCW" as an alternative title for "Digital Chart of the World.
     */
    private Collection<InternationalString> alternateTitles;

    /**
     * Reference date for the cited resource.
     */
    private Collection<CitationDate> dates;

    /**
     * Version of the cited resource.
     */
    private InternationalString edition;

    /**
     * Date of the edition in milliseconds elapsed sine January 1st, 1970,
     * or {@link Long#MIN_VALUE} if none.
     */
    private long editionDate;

    /**
     * Name and position information for an individual or organization that is responsible
     * for the resource. Returns an empty collection if there is none.
     */
    private Collection<ResponsibleParty> citedResponsibleParties;

    /**
     * Mode in which the resource is represented, or an empty collection if none.
     */
    private Collection<PresentationForm> presentationForms;

    /**
     * Information about the series, or aggregate dataset, of which the dataset is a part.
     * May be {@code null} if none.
     */
    private Series series;

    /**
     * Other information required to complete the citation that is not recorded elsewhere.
     * May be {@code null} if none.
     */
    private InternationalString otherCitationDetails;

    /**
     * Common title with holdings note. Note: title identifies elements of a series
     * collectively, combined with information about what volumes are available at the
     * source cited. May be {@code null} if there is no title.
     */
    private InternationalString collectiveTitle;

    /**
     * Constructs an initially empty citation.
     */
    public DefaultCitation() {
        editionDate = Long.MIN_VALUE;
    }

    /**
     * Constructs a citation with the specified title.
     *
     * @param title The title as a {@link String} or an {@link InternationalString} object,
     *        or {@code null} if none.
     */
    public DefaultCitation(final CharSequence title) {
        this(); // Initialize the date field.
        this.title = Types.toInternationalString(title);
    }

    /**
     * Constructs a citation with the specified responsible party.
     * This convenience constructor initializes the citation title
     * to the first non-null of the following properties:
     * {@linkplain DefaultResponsibleParty#getOrganisationName() organisation name},
     * {@linkplain DefaultResponsibleParty#getPositionName() position name} or
     * {@linkplain DefaultResponsibleParty#getIndividualName() individual name}.
     *
     * @param party The name and position information for an individual or organization that is
     *              responsible for the resource, or {@code null} if none.
     */
    public DefaultCitation(final ResponsibleParty party) {
        this(); // Initialize the date field.
        if (party != null) {
            citedResponsibleParties = singleton(party, ResponsibleParty.class);
            title = party.getOrganisationName();
            if (title == null) {
                title = party.getPositionName();
                if (title == null) {
                    String name = party.getIndividualName();
                    if (name != null) {
                        title = new SimpleInternationalString(name);
                    }
                }
            }
        }
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <cite>shallow</cite> copy constructor, since the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object The metadata to copy values from.
     *
     * @see #castOrCopy(Citation)
     */
    public DefaultCitation(final Citation object) {
        super(object);
        title                   = object.getTitle();
        alternateTitles         = copyCollection(object.getAlternateTitles(), InternationalString.class);
        dates                   = copyCollection(object.getDates(), CitationDate.class);
        edition                 = object.getEdition();
        editionDate             = toMilliseconds(object.getEditionDate());
        identifiers             = copyCollection(object.getIdentifiers(), Identifier.class);
        citedResponsibleParties = copyCollection(object.getCitedResponsibleParties(), ResponsibleParty.class);
        presentationForms       = copyCollection(object.getPresentationForms(), PresentationForm.class);
        series                  = object.getSeries();
        otherCitationDetails    = object.getOtherCitationDetails();
        collectiveTitle         = object.getCollectiveTitle();
// TODO ISBN                    = object.getISBN();
// TODO ISSN                    = object.getISSN();
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable actions in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code DefaultCitation}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code DefaultCitation} instance is created using the
     *       {@linkplain #DefaultCitation(Citation) copy constructor}
     *       and returned. Note that this is a <cite>shallow</cite> copy operation, since the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object The object to get as a SIS implementation, or {@code null} if none.
     * @return A SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static DefaultCitation castOrCopy(final Citation object) {
        if (object == null || object instanceof DefaultCitation) {
            return (DefaultCitation) object;
        }
        return new DefaultCitation(object);
    }

    /**
     * Returns the name by which the cited resource is known.
     */
    @Override
    @XmlElement(name = "title", required = true)
    public synchronized InternationalString getTitle() {
        return title;
    }

    /**
     * Sets the name by which the cited resource is known.
     *
     * @param newValue The new title, or {@code null} if none.
     */
    public synchronized void setTitle(final InternationalString newValue) {
        checkWritePermission();
        title = newValue;
    }

    /**
     * Returns the short name or other language name by which the cited information is known.
     * Example: "DCW" as an alternative title for "<cite>Digital Chart of the World</cite>".
     */
    @Override
    @XmlElement(name = "alternateTitle")
    public synchronized Collection<InternationalString> getAlternateTitles() {
        return alternateTitles = nonNullCollection(alternateTitles, InternationalString.class);
    }

    /**
     * Sets the short name or other language name by which the cited information is known.
     *
     * @param newValues The new alternate titles, or {@code null} if none.
     */
    public synchronized void setAlternateTitles(final Collection<? extends InternationalString> newValues) {
        alternateTitles = writeCollection(newValues, alternateTitles, InternationalString.class);
    }

    /**
     * Returns the reference date for the cited resource.
     */
    @Override
    @XmlElement(name = "date", required = true)
    public synchronized Collection<CitationDate> getDates() {
        return dates = nonNullCollection(dates, CitationDate.class);
    }

    /**
     * Sets the reference date for the cited resource.
     *
     * @param newValues The new dates, or {@code null} if none.
     */
    public synchronized void setDates(final Collection<? extends CitationDate> newValues) {
        dates = writeCollection(newValues, dates, CitationDate.class);
    }

    /**
     * Returns the version of the cited resource.
     */
    @Override
    @XmlElement(name = "edition")
    public synchronized InternationalString getEdition() {
        return edition;
    }

    /**
     * Sets the version of the cited resource.
     *
     * @param newValue The new edition, or {@code null} if none.
     */
    public synchronized void setEdition(final InternationalString newValue) {
        checkWritePermission();
        edition = newValue;
    }

    /**
     * Returns the date of the edition.
     */
    @Override
    @XmlElement(name = "editionDate")
    public synchronized Date getEditionDate() {
        return toDate(editionDate);
    }

    /**
     * Sets the date of the edition.
     *
     * @param newValue The new edition date, or {@code null} if none.
     */
    public synchronized void setEditionDate(final Date newValue) {
        checkWritePermission();
        editionDate = toMilliseconds(newValue);
    }

    /**
     * Returns the unique identifier for the resource.
     * Example: Universal Product Code (UPC), National Stock Number (NSN).
     *
     * {@section Unified identifiers view}
     * In this SIS implementation, the collection returned by this method includes the
     * {@linkplain #getISBN() ISBN} and {@linkplain #getISSN() ISSN} codes
     * (except at XML marshalling time for ISO 19139 compliance).
     */
    @Override
    @XmlElement(name = "identifier")
    public synchronized Collection<Identifier> getIdentifiers() {
        identifiers = nonNullCollection(identifiers, Identifier.class);
        return filterIdentifiers(identifiers);
    }

    /**
     * Sets the unique identifier for the resource.
     * Example: Universal Product Code (UPC), National Stock Number (NSN).
     *
     * <p>The following exceptions apply:</p>
     * <ul>
     *   <li>This method does not set the {@linkplain #getISBN() ISBN} and {@linkplain #getISSN() ISSN}
     *       codes, even if they are included in the given collection. The ISBN/ISSN codes shall be set
     *       by the {@link #setISBN(String)} or {@link #setISSN(String)} methods, for compliance with
     *       the ISO 19115 model.</li>
     *   <li>The {@linkplain IdentifierSpace XML identifiers} ({@linkplain IdentifierSpace#ID ID},
     *       {@linkplain IdentifierSpace#UUID UUID}, <i>etc.</i>) are ignored because.</li>
     * </ul>
     *
     * @param newValues The new identifiers, or {@code null} if none.
     */
    public synchronized void setIdentifiers(final Collection<? extends Identifier> newValues) {
        final Collection<Identifier> oldIds = NonMarshalledAuthority.getIdentifiers(identifiers);
        identifiers = writeCollection(newValues, identifiers, Identifier.class);
        NonMarshalledAuthority.setIdentifiers(identifiers, oldIds);
    }

    /**
     * Returns the name and position information for an individual or organization that is
     * responsible for the resource.
     */
    @Override
    @XmlElement(name = "citedResponsibleParty")
    public synchronized Collection<ResponsibleParty> getCitedResponsibleParties() {
        return citedResponsibleParties = nonNullCollection(citedResponsibleParties, ResponsibleParty.class);
    }

    /**
     * Sets the name and position information for an individual or organization that is responsible
     * for the resource.
     *
     * @param newValues The new cited responsible parties, or {@code null} if none.
     */
    public synchronized void setCitedResponsibleParties(final Collection<? extends ResponsibleParty> newValues) {
        citedResponsibleParties = writeCollection(newValues, citedResponsibleParties, ResponsibleParty.class);
    }

    /**
     * Returns the mode in which the resource is represented.
     */
    @Override
    @XmlElement(name = "presentationForm")
    public synchronized Collection<PresentationForm> getPresentationForms() {
        return presentationForms = nonNullCollection(presentationForms, PresentationForm.class);
    }

    /**
     * Sets the mode in which the resource is represented.
     *
     * @param newValues The new presentation form, or {@code null} if none.
     */
    public synchronized void setPresentationForms(final Collection<? extends PresentationForm> newValues) {
        presentationForms = writeCollection(newValues, presentationForms, PresentationForm.class);
    }

    /**
     * Returns the information about the series, or aggregate dataset, of which the dataset is a part.
     */
    @Override
    @XmlElement(name = "series")
    public synchronized Series getSeries() {
        return series;
    }

    /**
     * Sets the information about the series, or aggregate dataset, of which the dataset is a part.
     *
     * @param newValue The new series.
     */
    public synchronized void setSeries(final Series newValue) {
        checkWritePermission();
        series = newValue;
    }

    /**
     * Returns other information required to complete the citation that is not recorded elsewhere.
     */
    @Override
    @XmlElement(name = "otherCitationDetails")
    public synchronized InternationalString getOtherCitationDetails() {
        return otherCitationDetails;
    }

    /**
     * Sets other information required to complete the citation that is not recorded elsewhere.
     *
     * @param newValue Other citations details, or {@code null} if none.
     */
    public synchronized void setOtherCitationDetails(final InternationalString newValue) {
        checkWritePermission();
        otherCitationDetails = newValue;
    }

    /**
     * Returns the common title with holdings note. Note: title identifies elements of a series
     * collectively, combined with information about what volumes are available at the
     * source cited.
     */
    @Override
    @XmlElement(name = "collectiveTitle")
    public synchronized InternationalString getCollectiveTitle() {
        return collectiveTitle;
    }

    /**
     * Sets the common title with holdings note. This title identifies elements of a series
     * collectively, combined with information about what volumes are available at the source cited.
     *
     * @param newValue The new collective title, or {@code null} if none.
     */
    public synchronized void setCollectiveTitle(final InternationalString newValue) {
        checkWritePermission();
        collectiveTitle = newValue;
    }

    /**
     * Returns the International Standard Book Number.
     * In this SIS implementation, invoking this method is equivalent to:
     *
     * {@preformat java
     *   return getIdentifierMap().getSpecialized(Citations.ISBN);
     * }
     *
     * @see Citations#ISBN
     */
    @Override
    @XmlElement(name = "ISBN")
    public synchronized String getISBN() {
        return CollectionsExt.isNullOrEmpty(identifiers) ? null : getIdentifierMap().getSpecialized(ISBN);
    }

    /**
     * Sets the International Standard Book Number.
     * In this SIS implementation, invoking this method is equivalent to:
     *
     * {@preformat java
     *   getIdentifierMap().putSpecialized(Citations.ISBN, newValue);
     * }
     *
     * @param newValue The new ISBN, or {@code null} if none.
     */
    public synchronized void setISBN(final String newValue) {
        checkWritePermission();
        if (newValue != null || !CollectionsExt.isNullOrEmpty(identifiers)) {
            getIdentifierMap().putSpecialized(ISBN, newValue);
        }
    }

    /**
     * Returns the International Standard Serial Number.
     * In this SIS implementation, invoking this method is equivalent to:
     *
     * {@preformat java
     *   return getIdentifierMap().getSpecialized(Citations.ISSN);
     * }
     *
     * @see Citations#ISSN
     */
    @Override
    @XmlElement(name = "ISSN")
    public synchronized String getISSN() {
        return CollectionsExt.isNullOrEmpty(identifiers) ? null : getIdentifierMap().getSpecialized(ISSN);
    }

    /**
     * Sets the International Standard Serial Number.
     * In this SIS implementation, invoking this method is equivalent to:
     *
     * {@preformat java
     *   getIdentifierMap().putSpecialized(Citations.ISSN, newValue);
     * }
     *
     * @param newValue The new ISSN.
     */
    public synchronized void setISSN(final String newValue) {
        checkWritePermission();
        if (newValue != null || !CollectionsExt.isNullOrEmpty(identifiers)) {
            getIdentifierMap().putSpecialized(ISSN, newValue);
        }
    }
}
