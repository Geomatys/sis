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
package org.apache.sis.storage.base;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.opengis.util.CodeList;
import org.opengis.util.InternationalString;
import org.opengis.metadata.Metadata;
import org.opengis.metadata.citation.Citation;
import org.opengis.metadata.citation.CitationDate;
import org.opengis.metadata.citation.DateType;
import org.opengis.metadata.citation.Series;
import org.opengis.metadata.identification.Identification;
import org.opengis.metadata.lineage.Lineage;
import org.opengis.metadata.lineage.ProcessStep;
import org.opengis.metadata.acquisition.AcquisitionInformation;
import org.opengis.metadata.acquisition.Instrument;
import org.opengis.metadata.acquisition.Platform;
import org.opengis.metadata.spatial.CellGeometry;
import org.opengis.metadata.spatial.Georectified;
import org.opengis.metadata.spatial.SpatialRepresentation;
import org.opengis.metadata.spatial.GridSpatialRepresentation;
import org.apache.sis.util.collection.CodeListSet;

// Specific to the geoapi-3.1 and geoapi-4.0 branches:
import org.opengis.metadata.citation.Party;
import org.opengis.metadata.citation.Responsibility;


/**
 * Helper methods for fetching metadata to be written by {@code DataStore} implementations.
 * This is not a general-purpose builder suitable for public API, since the methods provided
 * in this class are tailored for Apache SIS data store needs.
 * API of this class may change in any future SIS versions.
 *
 * @author  Martin Desruisseaux (Geomatys)
 *
 * @param  <T>  type of temporal objects.
 */
public abstract class MetadataFetcher<T> {
    /**
     * Title of the resource, or {@code null} if none.
     *
     * <p>Path: {@code metadata/identificationInfo/citation/title}</p>
     */
    public List<String> title;

    /**
     * Name of the series of which the resource is a part, or {@code null} if none.
     *
     * <p>Path: {@code metadata/identificationInfo/citation/series/name}</p>
     */
    public List<String> series;

    /**
     * Details on which pages of the publication the resource was published, or {@code null} if none.
     *
     * <p>Path: {@code metadata/identificationInfo/citation/series/page}</p>
     */
    public List<String> page;

    /**
     * Names of the authors, or {@code null} if none.
     *
     * <p>Path: {@code metadata/identificationInfo/citation/party/name}</p>
     */
    public List<String> party;

    /**
     * Dates when the resource has been created, or {@code null} if none.
     * Limited to a singleton by default.
     *
     * <p>Path: {@code metadata/identificationInfo/citation/date}</p>
     */
    public List<T> creationDate;

    /**
     * Unique identification of the measuring instrument, or {@code null} if none.
     *
     * <p>Path: {@code metadata/acquisitionInformation/platform/instrument/identifier}</p>
     */
    public List<String> instrument;

    /**
     * Reference to document describing processing software, or {@code null} if none.
     *
     * <p>Path: {@code metadata/resourceLineage/processStep/processingInformation/softwareReference/title}</p>
     */
    public List<String> software;

    /**
     * Additional details about the processing procedure, or {@code null} if none.
     *
     * <p>Path: {@code metadata/resourceLineage/processStep/processingInformation/procedureDescription}</p>
     */
    public List<String> procedure;

    /**
     * General description of the transformation to a georectified grid, or {@code null} if none.
     *
     * <p>Path: {@code metadata/spatialRepresentationInfo/transformationDimensionDescription}</p>
     */
    public List<String> transformationDimension;

    /**
     * Whether grid data are representative of pixel areas or points, or {@code null} if none.
     */
    public Set<CellGeometry> cellGeometry;

    /**
     * The locale to use for converting international strings to {@link String} objects.
     * May also be used for date or number formatting.
     */
    protected final Locale locale;

    /**
     * Creates an initially empty metadata fetcher.
     *
     * @param  locale  the locale to use for converting international strings to {@link String} objects.
     */
    public MetadataFetcher(final Locale locale) {
        this.locale = locale;
    }

    /**
     * Invokes {@code this.accept(E)} for all elements in the given collection.
     * This method ignores null values (this is a paranoiac safety) and stops
     * the iteration if an {@code accept(E)} call returns {@code true}.
     *
     * @param  <E>       type of elements in the method.
     * @param  accept    the method to invoke for each element.
     * @param  elements  the collection of elements, or {@code null} if none.
     */
    private <E> void forEach(final BiPredicate<MetadataFetcher<T>,E> accept, final Iterable<? extends E> elements) {
        if (elements != null) {
            for (final E info : elements) {
                if (info != null && accept.test(this, info)) break;
            }
        }
    }

    /**
     * Fetches some properties from the given metadata object.
     * The default implementation iterates over the {@link Identification}, {@link Lineage} and
     * {@link AcquisitionInformation} objects, filters null values (this is a paranoiac safety),
     * then delegate to the corresponding {@code accept(…)} method.
     *
     * @param  info  the metadata, or {@code null} if none.
     */
    public void accept(final Metadata info) {
        if (info != null) {
            forEach(MetadataFetcher::accept, info.getIdentificationInfo());
            forEach(MetadataFetcher::accept, info.getResourceLineages());
            forEach(MetadataFetcher::accept, info.getAcquisitionInformation());
            forEach(MetadataFetcher::accept, info.getSpatialRepresentationInfo());
        }
    }

    /**
     * Fetches some properties from the given identification object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the identification object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final Identification info) {
        final Citation citation = info.getCitation();
        if (citation == null) {
            return false;
        }
        title = addString(title, citation.getTitle());
        final Series e = citation.getSeries();
        if (e != null) {
            series = addString(series, e.getName());
            page   = addString(page,   e.getPage());
        }
        forEach(MetadataFetcher::accept, citation.getCitedResponsibleParties());
        forEach(MetadataFetcher::accept, citation.getDates());
        return title != null;
    }

    /**
     * Fetches some properties from the given responsible party.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the responsible party (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final Responsibility info) {
        party = addStrings(party, info.getParties(), Party::getName);
        return false;
    }

    /**
     * Fetches some properties from the given resource citation date.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the resource citation date (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final CitationDate info) {
        if (creationDate == null) {
            if (info.getDateType() == DateType.CREATION) {
                creationDate = List.of(convertDate(info.getDate()));
            } else {
                return false;       // Search another date.
            }
        }
        return true;
    }

    /**
     * Fetches some properties from the given lineage object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the lineage object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final Lineage info) {
        forEach(MetadataFetcher::accept, info.getProcessSteps());
        return false;
    }

    /**
     * Fetches some properties from the given process step.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the process step object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final ProcessStep info) {
        final var processing = info.getProcessingInformation();
        if (processing != null) {
            software  = addStrings(software,  processing.getSoftwareReferences(), Citation::getTitle);
            procedure = addString (procedure, processing.getProcedureDescription());
        }
        return false;
    }

    /**
     * Fetches some properties from the given acquisition object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the acquisition object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final AcquisitionInformation info) {
        forEach(MetadataFetcher::accept, info.getPlatforms());
        return false;
    }

    /**
     * Fetches some properties from the given platform object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the platform object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final Platform info) {
        forEach(MetadataFetcher::accept, info.getInstruments());
        return false;
    }

    /**
     * Fetches some properties from the given instrument object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the instrument object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final Instrument info) {
        final var id = info.getIdentifier();
        if (id != null) {
            instrument = addString(instrument, id.getCode());
        }
        return false;
    }

    /**
     * Fetches some properties from the given spatial representation object.
     * Subclasses can override if they need to fetch more details.
     *
     * @param  info  the spatial representation object (not null).
     * @return whether to stop iteration after the given object.
     */
    protected boolean accept(final SpatialRepresentation info) {
        if (info instanceof GridSpatialRepresentation) {
            addCode(CellGeometry.class, cellGeometry, ((GridSpatialRepresentation) info).getCellGeometry());
            if (info instanceof Georectified) {
                addString(transformationDimension, ((Georectified) info).getTransformationDimensionDescription());
            }
        }
        return false;
    }

    /**
     * Adds all international strings in the given collection.
     *
     * @param  <E>     type of elements in the collection.
     * @param  target  where to add strings, or {@code null} if not yet created.
     * @param  source  elements from which to get international string, or {@code null} if none.
     * @param  getter  method to invoke on each element for getting the international string.
     * @return the collection where strings were added.
     */
    private <E> List<String> addStrings(List<String> target, final Iterable<? extends E> source,
                                        final Function<E,InternationalString> getter)
    {
        if (source != null) {
            for (final E e : source) {
                if (e != null) {
                    target = addString(target, getter.apply(e));
                }
            }
        }
        return target;
    }

    /**
     * Adds the given international string in the given collection.
     *
     * @param  target  where to add the string, or {@code null} if not yet created.
     * @param  value   the international string to add, or {@code null} if none.
     * @return the collection where the string was added.
     */
    private List<String> addString(List<String> target, final InternationalString value) {
        if (value != null) {
            target = addString(target, value.toString(locale));
        }
        return target;
    }

    /**
     * Adds the given string in the given collection.
     *
     * @param  target  where to add the string, or {@code null} if not yet created.
     * @param  value   the string to add, or {@code null} if none.
     * @return the collection where the string was added.
     */
    private static List<String> addString(List<String> target, String value) {
        if (value != null && !(value = value.trim()).isEmpty()) {
            if (target == null) {
                target = new ArrayList<>(2);        // We will usually have only one element.
            }
            target.add(value);
        }
        return target;
    }

    /**
     * Adds the given code in the given collection.
     *
     * @param  <E>     compile-time value of {@code type} argument.
     * @param  type    type of code list elements.
     * @param  target  collection where to add the code.
     * @param  value   the code to add.
     * @return the collection where to code was added.
     */
    private static <E extends CodeList<E>> Set<E> addCode(final Class<E> type, Set<E> target, final E value) {
        if (value != null) {
            if (target == null) {
                target = new CodeListSet<>(type);
            }
            target.add(value);
        }
        return target;
    }

    /**
     * Converts the given date into the object to store.
     * The {@code <T>} type may be for example {@code <String>}
     * with a string representation specified by the format implemented by the store.
     *
     * @param  date  the date to convert.
     * @return subclass-dependent object representing the given date.
     */
    protected abstract T convertDate(final Date date);
}
