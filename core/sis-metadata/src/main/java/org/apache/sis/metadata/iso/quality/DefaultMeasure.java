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

import java.util.Collection;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.opengis.util.InternationalString;
import org.opengis.util.TypeName;
import org.opengis.metadata.quality.Measure;
import org.apache.sis.metadata.iso.ISOMetadata;
import org.apache.sis.internal.jaxb.FilterByVersion;
import org.opengis.metadata.Identifier;
import org.opengis.metadata.quality.BasicMeasure;
import org.opengis.metadata.quality.Description;
import org.opengis.metadata.quality.Parameter;
import org.opengis.metadata.quality.SourceReference;
import org.opengis.metadata.quality.ValueStructure;


/**
 * Data quality measure.
 * The following property is mandatory in a well-formed metadata according ISO 19115:
 *
 * <div class="preformat">{@code DQM_Measure}
 * {@code   └─measureIdentifier……………} Value uniquely identifying the measure within a namespace.
 * {@code   ├─name……………} Name of the data quality measure applied to the data.
 * {@code   ├─elementName……………} Name of the data quality element for which quality is reported.
 * {@code   ├─definition……………} Definition of the fundamental concept for the data quality measure.
 * {@code   ├─valueType……………} Value type for reporting a data quality result (shall be one of the data types defined in ISO/19103:2005).</div>
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
 * @author  Alexis Gaillard (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
@XmlType(name = "DQM_Measure_Type", propOrder = {
    "measureIdentifier",
    "name",
    "alias",
    "elementName",
    "definition",
    "description",
    "valueType",
    "valueStructure",
    "example",
    "basicMeasure",
    "sourceReference",
    "parameter"
})
@XmlRootElement(name = "DQM_Measure")
public class DefaultMeasure extends ISOMetadata implements Measure {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -2004468907779670827L;
    /**
     * Value uniquely identifying the measure within a namespace.
     */
    private Identifier measureIdentifier;

    /**
     * Name of the data quality measure applied to the data.
     */
    private InternationalString name;

    /**
     * Another recognized name, an abbreviation or a short name for the same data quality measure.
     */
    private InternationalString alias;

    /**
     * Name of the data quality element for which quality is reported.
     */
    private TypeName elementName;

    /**
     * Definition of the fundamental concept for the data quality measure.
     */
    private InternationalString definition;

    /**
     *  Description of the data quality measure, including all formulae and/
     *  or illustrations needed to establish the result of applying the measure.
     */
    private Description description;

    /**
     * Value type for reporting a data quality result (shall be one of the data types defined in ISO/19103:2005).
     */
    private TypeName valueType;

    /**
     * Structure for reporting a complex data quality result.
     */
    private ValueStructure valueStructure;

    /**
     * Illustration of the use of a data quality measure.
     */
    private Collection<Description> example;

    /**
     * Definition of the fundamental concept for the data quality measure.
     */
    private BasicMeasure basicMeasure;

    /**
     * Reference to the source of an item that has been adopted from an external source.
     */
    private Collection<SourceReference> sourceReference;

    /**
     * Auxiliary variable used by the data quality measure, including its name, definition and optionally its description.
     */
    private Collection<Parameter> parameter;

    /**
     * Constructs an initially empty element.
     */
    public DefaultMeasure() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <cite>shallow</cite> copy constructor, since the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(Measure)
     */
    @SuppressWarnings("unchecked")
    public DefaultMeasure(final Measure object) {
        super(object);
        if (object != null) {
            measureIdentifier  = object.getMeasureIdentifier();
            name               = object.getName();
            alias              = object.getAlias();
            elementName        = object.getElementName();
            definition         = object.getDefinition();
            description        = object.getDescription();
            valueType          = object.getValueType();
            valueStructure     = object.getValueStructure();
            example            = (Collection<Description>) object.getExample();
            basicMeasure       = object.getBasicMeasure();
            sourceReference    = (Collection<SourceReference>) object.getSourceReference();
            parameter          = (Collection<Parameter>) object.getParameter();
        }
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is an instance of {@link PositionalAccuracy},
     *       {@link TemporalAccuracy}, {@link ThematicAccuracy}, {@link LogicalConsistency},
     *       {@link Completeness} or {@link Usability}, then this method delegates to the
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
    public static DefaultMeasure castOrCopy(final Measure object) {

        if (object instanceof DefaultMeasure) {
            return DefaultMeasure.castOrCopy(object);
        }
        return new DefaultMeasure(object);
    }

    /**
     * Returns the value uniquely identifying the measure within a namespace.
     *
     * @return value uniquely identifying the measure within a namespace, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "measureIdentifier")
    public Identifier getMeasureIdentifier() {
        return FilterByVersion.CURRENT_METADATA.accept() ? measureIdentifier : null;
    }

    /**
     * Sets the value uniquely identifying the measure within a namespace.
     *
     * @param  newValue  the new measure identification.
     */
    public void setMeasureIdentifier(final Identifier newValue)  {
        checkWritePermission(measureIdentifier);
        measureIdentifier = newValue;
    }

    /**
     * Returns the name of the data quality measure applied to the data.
     *
     * @return name of the data quality measure applied to the data, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "name")
    public InternationalString getName() {
        return FilterByVersion.CURRENT_METADATA.accept() ? name : null;
    }

    /**
     * Sets the name of the data quality measure applied to the data.
     *
     * @param  newValue  the new measure name.
     */
    public void setName(final InternationalString newValue)  {
        checkWritePermission(name);
        name = newValue;
    }

    /**
     * Returns the another recognized name, an abbreviation or a short name for the same data quality measure.
     *
     * @return another recognized name, an abbreviation or a short name for the same data quality measure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "alias")
    public InternationalString getAlias() {
        return FilterByVersion.CURRENT_METADATA.accept() ? alias : null;
    }

    /**
     * Sets the another recognized name, an abbreviation or a short name for the same data quality measure.
     *
     * @param  newValue  the new measure alias.
     */
    public void setAlias(final InternationalString newValue)  {
        checkWritePermission(alias);
        alias = newValue;
    }

    /**
     * Returns the name of the data quality element for which quality is reported.
     *
     * @return name of the data quality element for which quality is reported, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "elementName")
    public TypeName getElementName() {
        return FilterByVersion.CURRENT_METADATA.accept() ? elementName : null;
    }

    /**
     * Sets the name of the data quality element for which quality is reported.
     *
     * @param  newValue  the new measure element name.
     */
    public void setElementName(final TypeName newValue)  {
        checkWritePermission(elementName);
        elementName = newValue;
    }

    /**
     * Returns the definition of the fundamental concept for the data quality measure.
     *
     * @return definition of the fundamental concept for the data quality measure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "definition")
    public InternationalString getDefinition() {
        return FilterByVersion.CURRENT_METADATA.accept() ? definition : null;
    }

    /**
     * Sets the definition of the fundamental concept for the data quality measure.
     *
     * @param  newValue  the new measure definition.
     */
    public void setDefinition(final InternationalString newValue)  {
        checkWritePermission(definition);
        definition = newValue;
    }

    /**
     * Returns the description of the data quality measure, including all formulae and/
     * or illustrations needed to establish the result of applying the measure.
     *
     * @return description of the data quality measure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "description")
    public Description getDescription() {
       return FilterByVersion.CURRENT_METADATA.accept() ? description : null;
    }

    /**
     * Sets the description of the data quality measure, including all formulae and/
     * or illustrations needed to establish the result of applying the measure.
     *
     * @param  newValue  the new measure description.
     */
    public void setDescription(final Description newValue)  {
        checkWritePermission(description);
        description = newValue;
    }

    /**
     * Returns the value type for reporting a data quality result (shall be one of the data types defined in ISO/19103:2005).
     *
     * @return value type for reporting a data quality result, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "valueType")
    public TypeName getValueType() {
        return FilterByVersion.CURRENT_METADATA.accept() ? valueType : null;
    }

    /**
     * Sets the value type for reporting a data quality result (shall be one of the data types defined in ISO/19103:2005).
     *
     * @param  newValue  the new measure value type.
     */
    public void setValueType(final TypeName newValue)  {
        checkWritePermission(valueType);
        valueType = newValue;
    }

    /**
     * Returns the structure for reporting a complex data quality result.
     *
     * @return structure for reporting a complex data quality result, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "valueStructure")
    public ValueStructure getValueStructure() {
        return FilterByVersion.CURRENT_METADATA.accept() ? valueStructure : null;
    }

    /**
     * Sets the structure for reporting a complex data quality result.
     *
     * @param  newValue  the new measure value structure.
     */
    public void setValueStructure(final ValueStructure newValue)  {
        checkWritePermission(valueStructure);
        valueStructure = newValue;
    }

    /**
     * Returns the illustration of the use of a data quality measure.
     *
     * @return illustration of the use of a data quality measure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "example")
    public Collection<Description> getExample() {
        if (!FilterByVersion.CURRENT_METADATA.accept()) return null;
        return example = nonNullCollection(example, Description.class);
    }

    /**
     * Sets the illustration of the use of a data quality measure.
     *
     * @param  newValues  the new name of example.
     */
    public void setExample(final Collection<? extends Description> newValues) {
        example = writeCollection(newValues, example, Description.class);
    }

    /**
     * Returns the definition of the fundamental concept for the data quality measure.
     *
     * @return definition of the fundamental concept for the data quality measure, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "basicMeasure")
    public BasicMeasure getBasicMeasure() {
        return FilterByVersion.CURRENT_METADATA.accept() ? basicMeasure : null;
    }

    /**
     * Sets the definition of the fundamental concept for the data quality measure.
     *
     * @param  newValue  the new basic measure.
     */
    public void setBasicMeasure(final BasicMeasure newValue)  {
        checkWritePermission(basicMeasure);
        basicMeasure = newValue;
    }

    /**
     * Returns the reference to the source of an item that has been adopted from an external source.
     *
     * @return reference to the source of an item that has been adopted from an external source, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "sourceReference")
    public Collection<SourceReference> getSourceReference() {
        if (!FilterByVersion.CURRENT_METADATA.accept()) return null;
        return sourceReference = nonNullCollection(sourceReference, SourceReference.class);
    }

    /**
     * Sets the reference to the source of an item that has been adopted from an external source.
     *
     * @param  newValues  the new source reference.
     */
    public void setSourceReference(final Collection<? extends SourceReference> newValues) {
        sourceReference = writeCollection(newValues, sourceReference, SourceReference.class);
    }

    /**
     * Returns the auxiliary variable used by the data quality measure, including its name, definition and optionally its description.
     *
     * @return auxiliary variable used by the data quality measure including its name, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "parameter")
    public Collection<Parameter> getParameter() {
        if (!FilterByVersion.CURRENT_METADATA.accept()) return null;
        return parameter = nonNullCollection(parameter, Parameter.class);
    }

    /**
     * Sets the auxiliary variable used by the data quality measure, including its name, definition and optionally its description.
     *
     * @param  newValues  the new measure parameter.
     */
    public void setParameter(final Collection<? extends Parameter> newValues) {
        parameter = writeCollection(newValues, parameter, Parameter.class);
    }
}
