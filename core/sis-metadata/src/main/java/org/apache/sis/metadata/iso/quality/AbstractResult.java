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

import java.util.Date;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlRootElement;
import org.opengis.metadata.quality.Result;
import org.opengis.metadata.quality.CoverageResult;
import org.opengis.metadata.quality.ConformanceResult;
import org.opengis.metadata.quality.QuantitativeResult;
import org.opengis.metadata.quality.DescriptiveResult;
import org.opengis.metadata.maintenance.Scope;
import org.apache.sis.metadata.iso.ISOMetadata;


/**
 * Generalization of more specific result classes.
 * The following properties are mandatory in a well-formed metadata according ISO 19115:
 *
 * <div class="preformat">{@code DQ_QuantitativeResult}
 * {@code   └─resultScope………………………………} Scope of the result.
 * {@code   ├─dateTime……………………} Date when the result was generated.</div>
 *
 * <p><b>Limitations:</b></p>
 * <ul>
 *   <li>Instances of this class are not synchronized for multi-threading.
 *       Synchronization, if needed, is caller's responsibility.</li>
 *   <li>Serialized objects of this class are not guaranteed to be compatible with future Apache SIS releases.
 *       Serialization support is appropriate for short term storage or RMI between applications running the
 *       same version of Apache SIS. For long term storage, use {@link org.apache.sis.xml.XML} instead.</li>
 * </ul>
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @author  Touraïvane (IRD)
 * @author  Alexis Gaillard (Geomatys)
 * @version 1.1
 * @since   0.3
 * @module
 */
@XmlType(name = "AbstractDQ_Result_Type", propOrder = {
    "resultScope",
    "dateTime"
})
@XmlRootElement(name = "AbstractDQ_Result")
@XmlSeeAlso({
    DefaultConformanceResult.class,
    DefaultQuantitativeResult.class,
    DefaultCoverageResult.class,
    DefaultDescriptiveResult.class
})
public class AbstractResult extends ISOMetadata implements Result {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 3510023908820052467L;

    /**
     * Scope of the result.
     */
    private Scope resultScope;

    /**
     * Date when the result was generated.
     */
    private Date dateTime;

    /**
     * Constructs an initially empty result.
     */
    public AbstractResult() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <cite>shallow</cite> copy constructor, since the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param  object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(Result)
     */
    public AbstractResult(final Result object) {
        super(object);
        if (object != null) {
            resultScope        = object.getResultScope();
            dateTime           = object.getDateTime();
        }
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is an instance of {@link ConformanceResult},
     *       {@link QuantitativeResult} or {@link DescriptiveResult}, then this method delegates to
     *       the {@code castOrCopy(…)} method of the corresponding SIS subclass.
     *       Note that if the given object implements more than one of the above-cited interfaces,
     *       then the {@code castOrCopy(…)} method to be used is unspecified.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code AbstractResult}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code AbstractResult} instance is created using the
     *       {@linkplain #AbstractResult(Result) copy constructor}
     *       and returned. Note that this is a <cite>shallow</cite> copy operation, since the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static AbstractResult castOrCopy(final Result object) {
        if (object instanceof CoverageResult) {
            return DefaultCoverageResult.castOrCopy((CoverageResult) object);
        }
        if (object instanceof QuantitativeResult) {
            return DefaultQuantitativeResult.castOrCopy((QuantitativeResult) object);
        }
        if (object instanceof ConformanceResult) {
            return DefaultConformanceResult.castOrCopy((ConformanceResult) object);
        }
        if (object instanceof DescriptiveResult) {
            return DefaultDescriptiveResult.castOrCopy((DescriptiveResult) object);
        }
        // Intentionally tested after the sub-interfaces.
        if (object == null || object instanceof AbstractResult) {
            return (AbstractResult) object;
        }
        return new AbstractResult(object);
    }

    /**
     * Returns the scope of the result.
     *
     * @return returns scope of the result.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "resultScope")
    public Scope getResultScope() {
        return resultScope;
    }

    /**
     * Sets the scope of the result.
     *
     * @param  newValue  the new evaluation procedure.
     */
    public void setResultScope(final Scope newValue) {
        resultScope = newValue;
    }

    /**
     * Returns the date or range of dates on which a data quality measure was applied.
     * The collection size is 1 for a single date, or 2 for a range.
     * Returns an empty collection if this information is not available.
     *
     * @return date or range of dates on which a data quality measure was applied.
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "dateTime")
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public Date getDateTime() {
        return dateTime;
    }

    /**
     * Sets the date or range of dates on which a data quality measure was applied.
     * The collection size is 1 for a single date, or 2 for a range.
     *
     * @param  newValues  the new dates, or {@code null}.
     */
    public void setDateTime(final Date newValues) {
        dateTime = newValues;
    }
}
