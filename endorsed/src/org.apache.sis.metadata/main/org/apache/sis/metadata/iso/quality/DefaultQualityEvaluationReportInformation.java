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
package org.apache.sis.metadata.iso.quality;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.opengis.util.InternationalString;
import org.opengis.metadata.citation.Citation;

// Specific to the geoapi-3.1 and geoapi-4.0 branches:
import org.opengis.metadata.quality.QualityEvaluationReportInformation;
import java.util.Collection;


/**
 * Reference to an quality evaluation report.
 * See the {@link QualityEvaluationReportInformation} GeoAPI interface for more details.
 * The following property is mandatory in a well-formed metadata according ISO 19157:
 *
 * <div class="preformat">{@code DQ_Element}
 * {@code   ├─reportReference……} Reference to the procedure information.
 * {@code   └─abstract………………………} Description of the evaluation method.</div>
 *
 * <h2>Limitations</h2>
 * <ul>
 *   <li>Instances of this class are not synchronized for multi-threading.
 *       Synchronization, if needed, is caller's responsibility.</li>
 *   <li>Serialized objects of this class are not guaranteed to be compatible with future Apache SIS releases.
 *       Serialization support is appropriate for short term storage or RMI between applications running the
 *       same version of Apache SIS. For long term storage, use {@link org.apache.sis.xml.XML} instead.</li>
 * </ul>
 *
 * @author  Alexis Gaillard (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.4
 * @since   1.3
 */
@XmlType(name = "DQ_StandaloneQualityReportInformation_Type", propOrder = {
    "reportReference",
    "abstract"
})
@XmlRootElement(name = "DQ_StandaloneQualityReportInformation")
public class DefaultQualityEvaluationReportInformation extends ISOMetadata implements QualityEvaluationReportInformation {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -5457365506126993719L;

    /**
     * Reference to the associated quality evaluation report.
     */
    @SuppressWarnings("serial")
    private Citation reportReference;

    /**
     * Abstract for the associated quality evaluation report.
     */
    @SuppressWarnings("serial")
    private InternationalString summary;

    /**
     * Reference to original results in the associated quality evaluation report.
     *
     * @since 4.0
     *
     * @return details of the quality evaluation report, or {@code null} if none.
     */

//    todo: there is a discrepancy in the spec between UML and Annex table. This field is tagged as a single value in
//    todo: the UML and as a collection in the table. Here we follow the API, Annex examples (section D.3.5) are not really clear.
    Collection<InternationalString> qualityEvaluationReportDetails;


    /**
     * Constructs an initially empty quality evaluation report information.
     */
    public DefaultQualityEvaluationReportInformation() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <em>shallow</em> copy constructor, because the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(QualityEvaluationReportInformation)
     */
    public DefaultQualityEvaluationReportInformation(final QualityEvaluationReportInformation object) {
        super(object);
        if (object != null) {
            reportReference  = object.getReportReference();
            summary          = object.getAbstract();
            qualityEvaluationReportDetails = copyCollection(object.getQualityEvaluationReportDetails(), InternationalString.class);
        }
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code DefaultQualityEvaluationReportInformation}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code DefaultQualityEvaluationReportInformation} instance is created using the
     *       {@linkplain #DefaultQualityEvaluationReportInformation(QualityEvaluationReportInformation) copy constructor}
     *       and returned. Note that this is a <em>shallow</em> copy operation, because the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static DefaultQualityEvaluationReportInformation castOrCopy(final QualityEvaluationReportInformation object) {
        if (object instanceof QualityEvaluationReportInformation) {
            return DefaultQualityEvaluationReportInformation.castOrCopy((DefaultQualityEvaluationReportInformation) object);
        }
        return new DefaultQualityEvaluationReportInformation(object);
    }

    /**
     * Returns the reference to the associated quality evaluation report.
     *
     * @return reference of the quality evaluation report.
     */
    @Override
    @XmlElement(name = "reportReference", required = true)
    public Citation getReportReference() {
        return reportReference;
    }

    /**
     * Sets the reference to the associated quality evaluation report.
     *
     * @param  newValue  the new reference.
     */
    public void setReportReference(final Citation newValue) {
        checkWritePermission(reportReference);
        reportReference = newValue;
    }

    /**
     * Returns the abstract for the quality evaluation report.
     *
     * @return abstract of the quality evaluation report.
     */
    @Override
    @XmlElement(name = "abstract", required = true)
    public InternationalString getAbstract() {
        return summary;
    }

    /**
     * Sets the abstract for the associated quality evaluation report.
     *
     * @param  newValue  the new abstract.
     */
    public void setAbstract(final InternationalString newValue)  {
        checkWritePermission(summary);
        summary = newValue;
    }

    /**
     * Returns the reference to original results in the associated quality evaluation report.
     *
     * @since 4.0
     *
     * @return details of the quality evaluation report, or {@code null} if none.
     */
    @Override
    public Collection<InternationalString> getQualityEvaluationReportDetails() {
        return qualityEvaluationReportDetails;
    }

    /**
     * Sets the reference to original results in the associated quality evaluation report.
     *
     * @param  newValues  the new quality evaluation report details.
     *
     * @since 4.0
     */
    public void getQualityEvaluationReportDetails(Collection<InternationalString> newValues) {
        checkWritePermission(qualityEvaluationReportDetails);
        qualityEvaluationReportDetails = writeCollection(newValues, qualityEvaluationReportDetails, InternationalString.class);
    }

}
