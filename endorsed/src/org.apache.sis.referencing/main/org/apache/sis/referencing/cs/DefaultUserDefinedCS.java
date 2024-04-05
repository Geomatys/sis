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
package org.apache.sis.referencing.cs;

import java.util.Map;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.opengis.referencing.cs.CoordinateSystemAxis;


/**
 * A 2- or 3-dimensional coordinate system for any combination of coordinate axes not covered by other CS types.
 *
 * <table class="sis">
 * <caption>Permitted associations</caption>
 * <tr>
 *   <th>Used with CRS</th>
 *   <th>Permitted axis names</th>
 * </tr><tr>
 *   <td>{@linkplain org.apache.sis.referencing.crs.DefaultEngineeringCRS Engineering}</td>
 *   <td>unspecified</td>
 * </tr></table>
 *
 * <h2>Immutability and thread safety</h2>
 * This class is immutable and thus thread-safe if the property <em>values</em> (not necessarily the map itself)
 * and the {@link CoordinateSystemAxis} instances given to the constructor are also immutable. Unless otherwise
 * noted in the javadoc, this condition holds if all components were created using only SIS factories and static
 * constants.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @version 1.5
 * @since   0.4
 *
 * @deprecated The {@code UserDefinedCS} class has been removed from ISO 19111:2019.
 */
@Deprecated(since="1.5", forRemoval=true)   // Actually to be moved to an internal package for GML and WKT purposes.
@XmlType(name = "UserDefinedCSType")
@XmlRootElement(name = "UserDefinedCS")
public final class DefaultUserDefinedCS extends AbstractCS {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -4904091898305706316L;

    /**
     * Constructs a two-dimensional coordinate system from a set of properties.
     * The properties map is given unchanged to the
     * {@linkplain AbstractCS#AbstractCS(Map,CoordinateSystemAxis[]) super-class constructor}.
     * The following table is a reminder of main (not all) properties:
     *
     * <table class="sis">
     *   <caption>Recognized properties (non exhaustive list)</caption>
     *   <tr>
     *     <th>Property name</th>
     *     <th>Value type</th>
     *     <th>Returned by</th>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#NAME_KEY}</td>
     *     <td>{@link org.opengis.metadata.Identifier} or {@link String}</td>
     *     <td>{@link #getName()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#ALIAS_KEY}</td>
     *     <td>{@link org.opengis.util.GenericName} or {@link CharSequence} (optionally as array)</td>
     *     <td>{@link #getAlias()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#IDENTIFIERS_KEY}</td>
     *     <td>{@link org.opengis.metadata.Identifier} (optionally as array)</td>
     *     <td>{@link #getIdentifiers()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#REMARKS_KEY}</td>
     *     <td>{@link org.opengis.util.InternationalString} or {@link String}</td>
     *     <td>{@link #getRemarks()}</td>
     *   </tr>
     * </table>
     *
     * @param  properties  the properties to be given to the identified object.
     * @param  axis0       the first axis.
     * @param  axis1       the second axis.
     *
     * @see org.apache.sis.referencing.factory.GeodeticObjectFactory#createUserDefinedCS(Map, CoordinateSystemAxis, CoordinateSystemAxis)
     */
    public DefaultUserDefinedCS(final Map<String,?>   properties,
                                final CoordinateSystemAxis axis0,
                                final CoordinateSystemAxis axis1)
    {
        super(properties, axis0, axis1);
    }

    /**
     * Constructs a three-dimensional coordinate system from a set of properties.
     * The properties map is given unchanged to the
     * {@linkplain AbstractCS#AbstractCS(Map,CoordinateSystemAxis[]) super-class constructor}.
     *
     * @param  properties  the properties to be given to the identified object.
     * @param  axis0       the first axis.
     * @param  axis1       the second axis.
     * @param  axis2       the third axis.
     *
     * @see org.apache.sis.referencing.factory.GeodeticObjectFactory#createUserDefinedCS(Map, CoordinateSystemAxis, CoordinateSystemAxis, CoordinateSystemAxis)
     */
    public DefaultUserDefinedCS(final Map<String,?>   properties,
                                final CoordinateSystemAxis axis0,
                                final CoordinateSystemAxis axis1,
                                final CoordinateSystemAxis axis2)
    {
        super(properties, axis0, axis1, axis2);
    }

    /**
     * Creates a new CS derived from the specified one, but with different axis order or unit.
     *
     * @see #createForAxes(String, CoordinateSystemAxis[])
     */
    private DefaultUserDefinedCS(DefaultUserDefinedCS original, String name, CoordinateSystemAxis[] axes) {
        super(original, name, axes);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@inheritDoc}
     */
    @Override
    public DefaultUserDefinedCS forConvention(final AxesConvention convention) {
        return (DefaultUserDefinedCS) super.forConvention(convention);
    }

    /**
     * Returns a coordinate system with different axes.
     */
    @Override
    final AbstractCS createForAxes(final String name, final CoordinateSystemAxis[] axes) {
        switch (axes.length) {
            case 2: // Fall through
            case 3: return new DefaultUserDefinedCS(this, name, axes);
            default: throw unexpectedDimension(axes, 2, 3);
        }
    }




    /*
     ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     ┃                                                                                  ┃
     ┃                               XML support with JAXB                              ┃
     ┃                                                                                  ┃
     ┃        The following methods are invoked by JAXB using reflection (even if       ┃
     ┃        they are private) or are helpers for other methods invoked by JAXB.       ┃
     ┃        Those methods can be safely removed if Geographic Markup Language         ┃
     ┃        (GML) support is not needed.                                              ┃
     ┃                                                                                  ┃
     ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
     */

    /**
     * Constructs a new coordinate system in which every attributes are set to a null or empty value.
     * <strong>This is not a valid object.</strong> This constructor is strictly reserved to JAXB,
     * which will assign values to the fields using reflection.
     */
    private DefaultUserDefinedCS() {
    }
}
