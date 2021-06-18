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
import java.util.Collection;
import java.util.Collections;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import org.apache.sis.internal.jaxb.Context;
import org.opengis.metadata.Identifier;
import org.opengis.metadata.citation.Citation;
import org.opengis.metadata.quality.Result;
import org.opengis.metadata.quality.Element;
import org.opengis.metadata.quality.Usability;
import org.opengis.metadata.quality.Completeness;
import org.opengis.metadata.quality.TemporalAccuracy;
import org.opengis.metadata.quality.ThematicAccuracy;
import org.opengis.metadata.quality.PositionalAccuracy;
import org.opengis.metadata.quality.LogicalConsistency;
import org.opengis.metadata.quality.EvaluationMethodType;
import org.opengis.metadata.quality.EvaluationMethod;
import org.opengis.metadata.quality.MeasureReference;
import org.opengis.util.InternationalString;
import org.apache.sis.metadata.iso.ISOMetadata;
import org.apache.sis.internal.jaxb.FilterByVersion;
import org.apache.sis.internal.metadata.Dependencies;
import org.apache.sis.internal.xml.LegacyNamespaces;
import org.apache.sis.xml.Namespaces;

import static org.apache.sis.util.collection.Containers.isNullOrEmpty;


/**
 * Aspect of quantitative quality information.
 * The following property is mandatory in a well-formed metadata according ISO 19115:
 *
 * <div class="preformat">{@code DQ_Element}
 * {@code   └─results……………} Value obtained from applying a data quality measure.</div>
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
 * @author  Guilhem Legal (Geomatys)
 * @author  Alexis Gaillard (Geomatys)
 * @version 1.1
 * @since   0.3
 * @module
 */
@XmlType(name = "AbstractDQ_Element_Type", propOrder = {
    "namesOfMeasure",
    "measureIdentification",
    "measureDescription",
    "evaluationMethodType",
    "evaluationMethodDescription",
    "evaluationProcedure",
    "standaloneQualityReportDetails",
    "measure",
    "evaluationMethod",
    "results",
    "derivedElement",
    "dates"
})
@XmlRootElement(name = "AbstractDQ_Element")
@XmlSeeAlso({
    AbstractCompleteness.class,
    AbstractLogicalConsistency.class,
    AbstractPositionalAccuracy.class,
    AbstractThematicAccuracy.class,
    AbstractTemporalAccuracy.class,
    DefaultUsability.class
})
public class AbstractElement extends ISOMetadata implements Element {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -406229448295586970L;

    /**
     * Clause in the standaloneQualityReport where this data quality element or any
     * related data quality element (original results in case of derivation or aggregation) is described.
     */
    private InternationalString standaloneQualityReportDetails;

    /**
     * Reference to measure used.
     */
    private MeasureReference measure;

    /**
     * Evaluation information.
     */
    private EvaluationMethod evaluationMethod;

    /**
     * Value (or set of values) obtained from applying a data quality measure or the out
     * come of evaluating the obtained value (or set of values) against a specified
     * acceptable conformance quality level.
     */
    private Collection<Result> results;

    /**
     * In case of aggregation or derivation, indicates the original element.
     */
    private Collection<Element> derivedElement;

    /**
     * Constructs an initially empty element.
     */
    public AbstractElement() {
    }

    /**
     * Creates an element initialized to the given result.
     *
     * @param result  the value obtained from applying a data quality measure against a specified
     *                acceptable conformance quality level.
     */
    public AbstractElement(final Result result) {
        results = singleton(result, Result.class);
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <cite>shallow</cite> copy constructor, since the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(Element)
     */
    @SuppressWarnings("deprecation")
    public AbstractElement(final Element object) {
        super(object);
        if (object != null) {
            standaloneQualityReportDetails = object.getStandaloneQualityReportDetails();
            measure                        = object.getMeasure();
            evaluationMethod               = object.getEvaluationMethod();
            results                        = copyCollection(object.getResults(), Result.class);
            derivedElement                 = copyCollection(object.getDerivedElement(), Element.class);
        }
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is an instance of {@link PositionalAccuracy},
     *       {@link TemporalQuality}, {@link ThematicAccuracy}, {@link LogicalConsistency},
     *       {@link Completeness}, {@link Metaquality} or {@link UsabilityElement}, then this method delegates to the
     *       {@code castOrCopy(…)} method of the corresponding SIS subclass.
     *       Note that if the given object implements more than one of the above-cited interfaces,
     *       then the {@code castOrCopy(…)} method to be used is unspecified.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code AbstractElement}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code AbstractElement} instance is created using the
     *       {@linkplain #AbstractElement(Element) copy constructor}
     *       and returned. Note that this is a <cite>shallow</cite> copy operation, since the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static AbstractElement castOrCopy(final Element object) {
        if (object instanceof PositionalAccuracy) {
            return AbstractPositionalAccuracy.castOrCopy((PositionalAccuracy) object);
        }
        if (object instanceof TemporalAccuracy) {
            return AbstractTemporalAccuracy.castOrCopy((TemporalAccuracy) object);
        }
        if (object instanceof ThematicAccuracy) {
            return AbstractThematicAccuracy.castOrCopy((ThematicAccuracy) object);
        }
        if (object instanceof LogicalConsistency) {
            return AbstractLogicalConsistency.castOrCopy((LogicalConsistency) object);
        }
        if (object instanceof Completeness) {
            return AbstractCompleteness.castOrCopy((Completeness) object);
        }
        if (object instanceof Usability) {
            return DefaultUsability.castOrCopy((Usability) object);
        }
        // Intentionally tested after the sub-interfaces.
        if (object == null || object instanceof AbstractElement) {
            return (AbstractElement) object;
        }
        return new AbstractElement(object);
    }

    /**
     * Returns the name of the test applied to the data.
     *
     * @return name of the test applied to the data.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "nameOfMeasure", namespace = LegacyNamespaces.GMD)
    public Collection<InternationalString> getNamesOfMeasure() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (measure == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).namesOfMeasure;
            }
        } else {
            if (measure == null) {
                measure = new DefaultMeasureReference();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).getNamesOfMeasure();
            }
        }
        return Collections.unmodifiableCollection(measure.getNamesOfMeasure());
    }

    /**
     * Sets the name of the test applied to the data.
     *
     * @param  newValues  the new name of measures.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setNamesOfMeasure(final Collection<? extends InternationalString> newValues) {
        if (measure != null || !isNullOrEmpty(newValues)) {
            final DefaultMeasureReference impl;
            if (measure instanceof DefaultMeasureReference) {
                impl = (DefaultMeasureReference) measure;
            } else {
                impl = new DefaultMeasureReference(measure);
            }
            impl.setNamesOfMeasure(newValues);
            measure = impl;
        }
    }

    /**
     * Returns the code identifying a registered standard procedure, or {@code null} if none.
     *
     * @return code identifying a registered standard procedure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "measureIdentification", namespace = LegacyNamespaces.GMD)
    public Identifier getMeasureIdentification() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (measure == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).measureIdentification;
            }
        } else {
            if (measure == null) {
                measure = new DefaultMeasureReference();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).getMeasureIdentification();
            }
        }
        return measure.getMeasureIdentification();
    }

    /**
     * Sets the code identifying a registered standard procedure.
     *
     * @param  newValue  the new measure identification.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setMeasureIdentification(final Identifier newValue)  {
        if (measure != null || newValue != null) {
            final DefaultMeasureReference impl;
            if (measure instanceof DefaultMeasureReference) {
                impl = (DefaultMeasureReference) measure;
            } else {
                impl = new DefaultMeasureReference(measure);
            }
            impl.setMeasureIdentification(newValue);
            measure = impl;
        }
    }

    /**
     * Returns the description of the measure being determined.
     *
     * @return description of the measure being determined, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "measureDescription", namespace = LegacyNamespaces.GMD)
    public InternationalString getMeasureDescription() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (measure == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).measureDescription;
            }
        } else {
            if (measure == null) {
                measure = new DefaultMeasureReference();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultMeasureReference) measure).getMeasureDescription();
            }
        }
        return measure.getMeasureDescription();
    }

    /**
     * Sets the description of the measure being determined.
     *
     * @param  newValue  the new measure description.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setMeasureDescription(final InternationalString newValue)  {
       if (measure != null || newValue != null) {
            final DefaultMeasureReference impl;
            if (measure instanceof DefaultMeasureReference) {
                impl = (DefaultMeasureReference) measure;
            } else {
                impl = new DefaultMeasureReference(measure);
            }
            impl.setMeasureDescription(newValue);
            measure = impl;
        }
    }

    /**
     * Returns the type of method used to evaluate quality of the dataset.
     *
     * @return type of method used to evaluate quality, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "evaluationMethodType", namespace = LegacyNamespaces.GMD)
    public EvaluationMethodType getEvaluationMethodType() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (evaluationMethod == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (evaluationMethod instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).evaluationMethodType;
            }
        } else {
            if (evaluationMethod == null) {
                evaluationMethod = new DefaultEvaluationMethod();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).getEvaluationMethodType();
            }
        }
        return evaluationMethod.getEvaluationMethodType();
    }

    /**
     * Sets the type of method used to evaluate quality of the dataset.
     *
     * @param  newValue  the new evaluation method type.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setEvaluationMethodType(final EvaluationMethodType newValue)  {
        if (newValue != null || evaluationMethod != null) {
            final DefaultEvaluationMethod impl;
            if (evaluationMethod instanceof DefaultEvaluationMethod) {
                impl = (DefaultEvaluationMethod) evaluationMethod;
            } else {
                impl = new DefaultEvaluationMethod(evaluationMethod);
            }
            impl.setEvaluationMethodType(newValue);
            evaluationMethod = impl;
        }
    }

    /**
     * Returns the description of the evaluation method.
     *
     * @return description of the evaluation method, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "evaluationMethodDescription", namespace = LegacyNamespaces.GMD)
    public InternationalString getEvaluationMethodDescription() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (evaluationMethod == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (evaluationMethod instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).evaluationMethodDescription;
            }
        } else {
            if (evaluationMethod == null) {
                evaluationMethod = new DefaultEvaluationMethod();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).getEvaluationMethodDescription();
            }
        }
        return evaluationMethod.getEvaluationMethodDescription();
    }

    /**
     * Sets the description of the evaluation method.
     *
     * @param  newValue  the new evaluation method description.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setEvaluationMethodDescription(final InternationalString newValue)  {
        if (newValue != null || evaluationMethod != null) {
            final DefaultEvaluationMethod impl;
            if (evaluationMethod instanceof DefaultEvaluationMethod) {
                impl = (DefaultEvaluationMethod) evaluationMethod;
            } else {
                impl = new DefaultEvaluationMethod(evaluationMethod);
            }
            impl.setEvaluationMethodDescription(newValue);
            evaluationMethod = impl;
        }
    }

    /**
     * Returns the reference to the procedure information, or {@code null} if none.
     *
     * @return reference to the procedure information, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    @Override
    @XmlElement(name = "evaluationProcedure", namespace = LegacyNamespaces.GMD)
    public Citation getEvaluationProcedure() {
        final Context context = Context.current();
        if (Context.isFlagSet(context, Context.MARSHALLING)) {
            if (evaluationMethod == null || !Context.isFlagSet(context, Context.LEGACY_METADATA)) {
                return null;
            }
            if (evaluationMethod instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).evaluationProcedure;
            }
        } else {
            if (evaluationMethod == null) {
                evaluationMethod = new DefaultEvaluationMethod();
            }
            if (measure instanceof DefaultMeasureReference) {
                return ((DefaultEvaluationMethod) evaluationMethod).getEvaluationProcedure();
            }
        }
        return evaluationMethod.getEvaluationProcedure();
    }

    /**
     * Sets the reference to the procedure information.
     *
     * @param  newValue  the new evaluation procedure.
     *
     * @deprecated Removed From ISO_19157
     */
    @Deprecated
    public void setEvaluationProcedure(final Citation newValue) {
       if (newValue != null || evaluationMethod != null) {
            final DefaultEvaluationMethod impl;
            if (evaluationMethod instanceof DefaultEvaluationMethod) {
                impl = (DefaultEvaluationMethod) evaluationMethod;
            } else {
                impl = new DefaultEvaluationMethod(evaluationMethod);
            }
            impl.setEvaluationProcedure(newValue);
            evaluationMethod = impl;
        }
    }

    /**
     * Returns the date or range of dates on which a data quality measure was applied.
     * The collection size is 1 for a single date, or 2 for a range.
     * Returns an empty collection if this information is not available.
     *
     * @return date or range of dates on which a data quality measure was applied.
     *
     * @deprecated Moved to DefaultEvaluationMethod with ISO 19157.
     */
    @Deprecated
    @Dependencies("getEvaluationMethod")
    @XmlElement(name = "dateTime", namespace = Namespaces.GMD)
    public Collection<Date> getDates() {
        if (evaluationMethod == null) {
            evaluationMethod = new DefaultEvaluationMethod();
        }
        if (evaluationMethod instanceof DefaultEvaluationMethod) {
            return ((DefaultEvaluationMethod) evaluationMethod).getDates();
        } else {
            return Collections.unmodifiableCollection(evaluationMethod.getDates());
        }
    }

    /**
     * Sets the date or range of dates on which a data quality measure was applied.
     * The collection size is 1 for a single date, or 2 for a range.
     *
     * @param  newValues  the new dates, or {@code null}.
     *
     * @deprecated Moved to DefaultEvaluationMethod with ISO 19157.
     */
    @Deprecated
    public void setDates(final Collection<? extends Date> newValues) {
        if (evaluationMethod == null) {
            if (isNullOrEmpty(newValues)) {
                return;
            }
        }
        final DefaultEvaluationMethod dest;
        if (evaluationMethod == null) {
            dest = new DefaultEvaluationMethod();
        } else {
            dest = DefaultEvaluationMethod.castOrCopy(evaluationMethod);
        }
        dest.setDates(newValues);
        evaluationMethod = dest;
    }

    /**
     * Returns the clause in the standaloneQualityReport where this data quality element or any
     * related data quality element (original results in case of derivation or aggregation) is described.
     *
     * @return the clause in the standaloneQualityReport, or {@code null}.
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "standaloneQualityReportDetails")
    public InternationalString getStandaloneQualityReportDetails() {
        return FilterByVersion.CURRENT_METADATA.accept() ? standaloneQualityReportDetails : null;
    }

    /**
     * Sets the clause in the standaloneQualityReport where this data quality element or any
     * related data quality element (original results in case of derivation or aggregation) is described.
     *
     * @param  newValue  the clause in the standaloneQualityReport.
     */
    public void setStandaloneQualityReportDetails(final InternationalString newValue)  {
        checkWritePermission(standaloneQualityReportDetails);
        standaloneQualityReportDetails = newValue;
    }

    /**
     * Returns the reference to measure used.
     *
     * @return set of values obtained from applying a data quality measure.
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "measure", required = false)
    public MeasureReference getMeasure() {
        return FilterByVersion.CURRENT_METADATA.accept() ? measure : null;
    }

    /**
     * Sets the reference to measure used.
     *
     * @param  newValues  the new measure.
     */
    public void setMeasure(final MeasureReference newValues) {
        checkWritePermission(measure);
        measure = newValues;
    }

    /**
     * Returns the evaluation information..
     *
     * @return set of values obtained from applying a data quality measure.
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "evaluationMethod", required = false)
    public EvaluationMethod getEvaluationMethod() {
        return FilterByVersion.CURRENT_METADATA.accept() ? evaluationMethod : null;
    }

    /**
     * Sets the evaluation information..
     *
     * @param  newValues  the new results.
     */
    public void setEvaluationMethod(final EvaluationMethod newValues) {
        checkWritePermission(evaluationMethod);
        evaluationMethod = newValues;
    }

    /**
     * Returns the value (or set of values) obtained from applying a data quality measure or
     * the out come of evaluating the obtained value (or set of values) against a specified
     * acceptable conformance quality level.
     *
     * @return set of values obtained from applying a data quality measure.
     */
    @Override
    @XmlElement(name = "result", required = true)
    public Collection<Result> getResults() {
        return results = nonNullCollection(results, Result.class);
    }

    /**
     * Sets the value (or set of values) obtained from applying a data quality measure or
     * the out come of evaluating the obtained value (or set of values) against a specified
     * acceptable conformance quality level.
     *
     * @param  newValues  the new set of value.
     */
    public void setResults(final Collection<? extends Result> newValues) {
        results = writeCollection(newValues, results, Result.class);
    }

    /**
     * Returns the original element in case of aggregation or derivation.
     *
     * @return the elements.
     *
     * @since 1.1
     */
    @Override
    @XmlElement(name = "derivedElement")
    public Collection<Element> getDerivedElement() {
        if (!FilterByVersion.CURRENT_METADATA.accept()) return null;
        return derivedElement = nonNullCollection(derivedElement, Element.class);
    }

    /**
     * Sets the original element in case of aggregation or derivation.
     *
     * @param  newValues  the new elements.
     */
    public void setDerivedElement(final Collection<? extends Element> newValues) {
        derivedElement = writeCollection(newValues, derivedElement, Element.class);
    }
}
