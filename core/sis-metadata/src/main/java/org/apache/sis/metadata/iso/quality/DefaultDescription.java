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

import org.opengis.util.InternationalString;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.sis.internal.jaxb.FilterByVersion;
import org.apache.sis.internal.xml.LegacyNamespaces;
import org.apache.sis.metadata.iso.ISOMetadata;
import org.opengis.metadata.identification.BrowseGraphic;
import org.opengis.metadata.quality.Description;


/**
 * Data quality measure description.
 * The following property is mandatory in a well-formed metadata according ISO 19115:
 *
 * <div class="preformat">{@code DQM_Description}
 * {@code   └─textDescription……………} Text description.</div>
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
@XmlType(name = "DQM_Description_Type", propOrder = {
    "textDescription",
    "extendedDescription"
})
@XmlRootElement(name = "DQM_Description")
public class DefaultDescription extends ISOMetadata implements Description {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 4878784271547209576L;
    /**
     * Text description.
     */
    private InternationalString textDescription;

    /**
     * Illustration.
     */
    private BrowseGraphic extendedDescription;

    /**
     * Constructs an initially empty description.
     */
    public DefaultDescription() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <cite>shallow</cite> copy constructor, since the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(Description)
     */
    public DefaultDescription(final Description object) {
        super(object);
        if (object != null) {
            textDescription           = object.getTextDescription();
            extendedDescription       = object.getExtendedDescription();
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
    public static DefaultDescription castOrCopy(final Description object) {
        if (object instanceof DefaultDescription) {
            return DefaultDescription.castOrCopy(object);
        }
        return new DefaultDescription(object);
    }

    /**
     * Returns the text description.
     *
     * @return text description, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "textDescription")
    public InternationalString getTextDescription() {
        return FilterByVersion.CURRENT_METADATA.accept() ? textDescription : null;
    }

    /**
     * Sets the text description.
     *
     * @param  newValue  the new description text.
     */
    public void setTextDescription(final InternationalString newValue)  {
        checkWritePermission(textDescription);
        textDescription = newValue;
    }

    /**
     * Returns the illustration.
     *
     * @return the illustration, or {@code null}.
     *
     * @see <a href="https://issues.apache.org/jira/browse/SIS-394">Issue SIS-394</a>
     */
    @Override
    @XmlElement(name = "extendedDescription")
    public BrowseGraphic getExtendedDescription() {
        return FilterByVersion.CURRENT_METADATA.accept() ? extendedDescription : null;
    }

    /**
     * Sets the illustration.
     *
     * @param  newValue  the new description illustration.
     */
    public void setExtendedDescription(final BrowseGraphic newValue)  {
        checkWritePermission(extendedDescription);
        extendedDescription = newValue;
    }
}
