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
package org.apache.sis.referencing.datum;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.opengis.referencing.datum.Ellipsoid;
import org.opengis.referencing.datum.PrimeMeridian;
import org.opengis.referencing.datum.InertialReferenceFrame;
import org.apache.sis.io.wkt.Formatter;
import org.apache.sis.referencing.util.WKTKeywords;
import org.apache.sis.referencing.util.WKTUtilities;
import org.apache.sis.util.ComparisonMode;
import org.apache.sis.util.Utilities;
import org.apache.sis.xml.Namespaces;


/**
 * Origin and orientation of an inertial CRS.
 *
 * <h2>Immutability and thread safety</h2>
 * This class is immutable and thus thread-safe if the property <em>values</em> (not necessarily the map itself),
 * the {@link Ellipsoid} and the {@link PrimeMeridian} given to the constructor are also immutable.
 * Unless otherwise noted in the javadoc, this condition holds if all components were created using
 * only SIS factories and static constants.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version Testbed-19
 * @since   Testbed-19
 */
@XmlType(name = "InertialReferenceFrameType", namespace = Namespaces.GSP, propOrder = {
    "ellipsoid",
    "primeDirection"
})
@XmlRootElement(name = "InertialReferenceFrame", namespace = Namespaces.GSP)
public class DefaultInertialDatum extends AbstractDatum implements InertialReferenceFrame {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -7306364448437476042L;

    /**
     * The ellipsoid, or {@code null} if none.
     *
     * @see #getEllipsoid()
     */
    @SuppressWarnings("serial")             // Most SIS implementations are serializable.
    @XmlElement(name = "ellipsoid", namespace = Namespaces.GML)
    private final Ellipsoid ellipsoid;

    /**
     * The prime direction, or {@code null} if none.
     *
     * <p><b>Consider this field as final!</b>
     * This field is modified only at unmarshalling time by {@link #setPrimeMeridian(PrimeMeridian)}</p>
     *
     * @see #getPrimeDirection()
     */
    @SuppressWarnings("serial")             // Most SIS implementations are serializable.
    @XmlElement(name = "primeDirection")
    private final PrimeMeridian primeDirection;

    /**
     * Creates an inertial datum from the given properties. The properties map is given
     * unchanged to the {@linkplain AbstractDatum#AbstractDatum(Map) super-class constructor}.
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
     *     <td>{@link Identifier} or {@link String}</td>
     *     <td>{@link #getName()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#ALIAS_KEY}</td>
     *     <td>{@link GenericName} or {@link CharSequence} (optionally as array)</td>
     *     <td>{@link #getAlias()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#IDENTIFIERS_KEY}</td>
     *     <td>{@link Identifier} (optionally as array)</td>
     *     <td>{@link #getIdentifiers()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#DOMAINS_KEY}</td>
     *     <td>{@link org.opengis.referencing.ObjectDomain} (optionally as array)</td>
     *     <td>{@link #getDomains()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#REMARKS_KEY}</td>
     *     <td>{@link InternationalString} or {@link String}</td>
     *     <td>{@link #getRemarks()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.datum.Datum#ANCHOR_POINT_KEY}</td>
     *     <td>{@link InternationalString} or {@link String}</td>
     *     <td>{@link #getAnchorPoint()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.datum.Datum#REALIZATION_EPOCH_KEY}</td>
     *     <td>{@link Date}</td>
     *     <td>{@link #getRealizationEpoch()}</td>
     *   </tr>
     * </table>
     *
     * @param  properties     the properties to be given to the identified object.
     * @param  ellipsoid      the ellipsoid, or {@code null} if none
     * @param  primeDirection  the prime meridian, or {@code null} if none.
     */
    public DefaultInertialDatum(final Map<String,?> properties,
                                final Ellipsoid     ellipsoid,
                                final PrimeMeridian primeDirection)
    {
        super(properties);
        this.ellipsoid      = ellipsoid;
        this.primeDirection = primeDirection;
    }

    /**
     * Creates a new datum with the same values than the specified one.
     * This copy constructor provides a way to convert an arbitrary implementation into a SIS one
     * or a user-defined one (as a subclass), usually in order to leverage some implementation-specific API.
     *
     * <p>This constructor performs a shallow copy, i.e. the properties are not cloned.</p>
     *
     * @param  datum  the datum to copy.
     *
     * @see #castOrCopy(InertialReferenceFrame)
     */
    protected DefaultInertialDatum(final InertialReferenceFrame datum) {
        super(datum);
        ellipsoid      = datum.getEllipsoid().orElse(null);
        primeDirection = datum.getPrimeDirection().orElse(null);
    }

    /**
     * Returns a SIS datum implementation with the same values than the given arbitrary implementation.
     * If the given object is {@code null}, then this method returns {@code null}.
     * Otherwise if the given object is already a SIS implementation, then the given object is returned unchanged.
     * Otherwise a new SIS implementation is created and initialized to the attribute values of the given object.
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static DefaultInertialDatum castOrCopy(final InertialReferenceFrame object) {
        return (object == null) || (object instanceof DefaultInertialDatum)
                ? (DefaultInertialDatum) object : new DefaultInertialDatum(object);
    }

    /**
     * Returns the GeoAPI interface implemented by this class.
     * The SIS implementation returns {@code InertialReferenceFrame.class}.
     *
     * <h4>Note for implementers</h4>
     * Subclasses usually do not need to override this method since GeoAPI does not define {@code InertialReferenceFrame}
     * sub-interface. Overriding possibility is left mostly for implementers who wish to extend GeoAPI with their own set
     * of interfaces.
     *
     * @return {@code InertialReferenceFrame.class} or a user-defined sub-interface.
     */
    @Override
    public Class<? extends InertialReferenceFrame> getInterface() {
        return InertialReferenceFrame.class;
    }

    /**
     * Returns the ellipsoid given at construction time.
     *
     * @return the ellipsoid.
     */
    @Override
    public Optional<Ellipsoid> getEllipsoid() {
        return Optional.ofNullable(ellipsoid);
    }

    /**
     * Returns the prime direction given at construction time.
     *
     * @return the prime direction.
     */
    @Override
    public Optional<PrimeMeridian> getPrimeDirection() {
        return Optional.ofNullable(primeDirection);
    }

    /**
     * Compares this datum with the specified object for equality.
     *
     * @param  object  the object to compare to {@code this}.
     * @param  mode    {@link ComparisonMode#STRICT STRICT} for performing a strict comparison, or
     *                 {@link ComparisonMode#IGNORE_METADATA IGNORE_METADATA} for comparing only
     *                 properties relevant to coordinate transformations.
     * @return {@code true} if both objects are equal.
     */
    @Override
    public boolean equals(final Object object, final ComparisonMode mode) {
        if (object == this) {
            return true;                                // Slight optimization.
        }
        if (!super.equals(object, mode)) {
            return false;
        }
        switch (mode) {
            case STRICT: {
                final DefaultInertialDatum that = (DefaultInertialDatum) object;
                return Objects.equals(this.ellipsoid,     that.ellipsoid) &&
                       Objects.equals(this.primeDirection, that.primeDirection);
            }
            default: {
                final InertialReferenceFrame that = (InertialReferenceFrame) object;
                return Utilities.deepEquals(getEllipsoid().orElse(null),      that.getEllipsoid().orElse(null),      mode) &&
                       Utilities.deepEquals(getPrimeDirection().orElse(null), that.getPrimeDirection().orElse(null), mode);
            }
        }
    }

    /**
     * Invoked by {@code hashCode()} for computing the hash code when first needed.
     * See {@link org.apache.sis.referencing.AbstractIdentifiedObject#computeHashCode()}
     * for more information.
     *
     * @return the hash code value. This value may change in any future Apache SIS version.
     */
    @Override
    protected long computeHashCode() {
        return super.computeHashCode() + Objects.hashCode(ellipsoid) + 31 * Objects.hashCode(primeDirection);
    }

    /**
     * Formats this datum as a pseudo-<cite>Well Known Text</cite> {@code InertialDatum[…]} element.
     *
     * @return {@code "InertialDatum"}.
     */
    @Override
    protected String formatTo(final Formatter formatter) {
        super.formatTo(formatter);
        formatter.newLine();
        formatter.append(WKTUtilities.toFormattable(getEllipsoid().orElse(null)));
        formatter.append(WKTUtilities.toFormattable(getPrimeDirection().orElse(null)));
        formatter.newLine();                        // For writing the ID[…] element on its own line.
        formatter.setInvalidWKT(this, null);        // Because not an ISO 19162 object.
        return WKTKeywords.InertialDatum;
    }




    //////////////////////////////////////////////////////////////////////////////////////////////////
    ////////                                                                                  ////////
    ////////                               XML support with JAXB                              ////////
    ////////                                                                                  ////////
    ////////        The following methods are invoked by JAXB using reflection (even if       ////////
    ////////        they are private) or are helpers for other methods invoked by JAXB.       ////////
    ////////        Those methods can be safely removed if Geographic Markup Language         ////////
    ////////        (GML) support is not needed.                                              ////////
    ////////                                                                                  ////////
    //////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructs a new datum in which every attributes are set to a null value.
     * <strong>This is not a valid object.</strong> This constructor is strictly
     * reserved to JAXB, which will assign values to the fields using reflection.
     */
    private DefaultInertialDatum() {
        ellipsoid     = null;
        primeDirection = null;
    }
}
