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
import jakarta.xml.bind.annotation.XmlSeeAlso;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.opengis.metadata.quality.ThematicAccuracy;
import org.opengis.metadata.quality.ThematicClassificationCorrectness;
import org.opengis.metadata.quality.QuantitativeAttributeAccuracy;

// Specific to the geoapi-3.1 and geoapi-4.0 branches:
import org.opengis.metadata.quality.NonQuantitativeAttributeCorrectness;


/**
 * Accuracy and correctness of attributes and classification of features.
 * The following property is mandatory in a well-formed metadata according ISO 19157:
 *
 * <div class="preformat">{@code DQ_ThematicAccuracy}
 * {@code   └─result……………} Value obtained from applying a data quality measure.</div>
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
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @author  Touraïvane (IRD)
 * @version 1.4
 * @since   0.3
 */
@XmlType(name = "AbstractDQ_ThematicAccuracy_Type")
@XmlRootElement(name = "AbstractDQ_ThematicAccuracy")
@XmlSeeAlso({
    DefaultThematicClassificationCorrectness.class,
    DefaultNonQuantitativeAttributeCorrectness.class,
    DefaultQuantitativeAttributeAccuracy.class
})
public class AbstractThematicAccuracy extends AbstractQualityElement implements ThematicAccuracy {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 7256282057348615018L;

    /**
     * Constructs an initially empty thematic accuracy.
     */
    public AbstractThematicAccuracy() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <em>shallow</em> copy constructor, because the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param  object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(ThematicAccuracy)
     */
    public AbstractThematicAccuracy(final ThematicAccuracy object) {
        super(object);
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is an instance of {@link QuantitativeAttributeAccuracy},
     *       {@link NonQuantitativeAttributeCorrectness} or {@link ThematicClassificationCorrectness},
     *       then this method delegates to the {@code castOrCopy(…)} method of the corresponding
     *       SIS subclass. Note that if the given object implements more than one of the above-cited
     *       interfaces, then the {@code castOrCopy(…)} method to be used is unspecified.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code AbstractThematicAccuracy}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code AbstractThematicAccuracy} instance is created using the
     *       {@linkplain #AbstractThematicAccuracy(ThematicAccuracy) copy constructor} and returned.
     *       Note that this is a <em>shallow</em> copy operation, because the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static AbstractThematicAccuracy castOrCopy(final ThematicAccuracy object) {
        if (object instanceof QuantitativeAttributeAccuracy) {
            return DefaultQuantitativeAttributeAccuracy.castOrCopy((QuantitativeAttributeAccuracy) object);
        }
        if (object instanceof NonQuantitativeAttributeCorrectness) {
            return DefaultNonQuantitativeAttributeCorrectness.castOrCopy((NonQuantitativeAttributeCorrectness) object);
        }
        if (object instanceof ThematicClassificationCorrectness) {
            return DefaultThematicClassificationCorrectness.castOrCopy((ThematicClassificationCorrectness) object);
        }
        // Intentionally tested after the sub-interfaces.
        if (object == null || object instanceof AbstractThematicAccuracy) {
            return (AbstractThematicAccuracy) object;
        }
        return new AbstractThematicAccuracy(object);
    }
}
