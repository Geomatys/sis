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
package org.apache.sis.referencing.crs;

import java.util.Map;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.opengis.referencing.cs.CartesianCS;
import org.opengis.referencing.cs.MinkowskiCS;
import org.opengis.referencing.cs.SphericalCS;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.crs.InertialCRS;
import org.opengis.referencing.datum.InertialReferenceFrame;
import org.apache.sis.referencing.AbstractReferenceSystem;
import org.apache.sis.referencing.IdentifiedObjects;
import org.apache.sis.referencing.cs.AbstractCS;
import org.apache.sis.referencing.util.WKTKeywords;
import org.apache.sis.referencing.util.WKTUtilities;
import org.apache.sis.io.wkt.Formatter;
import org.apache.sis.xml.Namespaces;
import org.apache.sis.util.Classes;
import org.apache.sis.util.ArgumentChecks;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.metadata.internal.ImplementationHelper;


/**
 * A 2-, 3- or 4-dimensional coordinate reference system with axes at fixed position relative to stars.
 *
 * <h2>Immutability and thread safety</h2>
 * This class is immutable and thus thread-safe if the property <em>values</em> (not necessarily the map itself),
 * the coordinate system and the datum instances given to the constructor are also immutable. Unless otherwise noted
 * in the javadoc, this condition holds if all components were created using only SIS factories and static constants.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version Testbed-19
 *
 * @see org.apache.sis.referencing.factory.GeodeticAuthorityFactory#createInertialCRS(String)
 *
 * @since Testbed-19
 */
@XmlType(name = "InertialCRSType", namespace = Namespaces.GSP, propOrder = {
    "ellipsoidalCS",
    "cartesianCS",
    "sphericalCS",
    "minkowskiCS",
    "datum"
})
@XmlRootElement(name = "InertialCRS", namespace = Namespaces.GSP)
public class DefaultInertialCRS extends AbstractCRS implements InertialCRS {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 2558919903378843841L;

    /**
     * The datum.
     *
     * <p><b>Consider this field as final!</b>
     * This field is modified only at unmarshalling time by {@link #setDatum(InertialReferenceFrame)}</p>
     *
     * @see #getDatum()
     */
    @SuppressWarnings("serial")     // Most SIS implementations are serializable.
    private InertialReferenceFrame datum;

    /**
     * Creates a coordinate reference system from the given properties, datum and coordinate system.
     * The properties given in argument follow the same rules than for the
     * {@linkplain AbstractReferenceSystem#AbstractReferenceSystem(Map) super-class constructor}.
     * The following table is a reminder of main (not all) properties:
     *
     * <table class="sis">
     *   <caption>Recognized properties (non exhaustive list)</caption>
     *   <tr>
     *     <th>Property name</th>
     *     <th>Value type</th>
     *     <th>Returned by</th>
     *   </tr>
     *   <tr>
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
     *     <td>{@value org.opengis.referencing.IdentifiedObject#DOMAINS_KEY}</td>
     *     <td>{@link org.opengis.referencing.ObjectDomain} (optionally as array)</td>
     *     <td>{@link #getDomains()}</td>
     *   </tr><tr>
     *     <td>{@value org.opengis.referencing.IdentifiedObject#REMARKS_KEY}</td>
     *     <td>{@link org.opengis.util.InternationalString} or {@link String}</td>
     *     <td>{@link #getRemarks()}</td>
     *   </tr>
     * </table>
     *
     * @param  properties  the properties to be given to the coordinate reference system.
     * @param  datum       the datum.
     * @param  cs          the two-, three-dimensional or four-dimensional coordinate system.
     */
    public DefaultInertialCRS(final Map<String,?> properties,
                              final InertialReferenceFrame datum,
                              final CoordinateSystem cs)
    {
        super(properties, cs);
        ArgumentChecks.ensureNonNull("datum", datum);
        this.datum = datum;
        if (!Classes.isAssignableToAny(cs.getClass(), PERMITTED_CS_TYPES)) {
            final String title = IdentifiedObjects.getDisplayName(cs, null);
            throw new IllegalArgumentException(Errors.format(Errors.Keys.IllegalCoordinateSystem_1, title));
        }
    }

    /**
     * Creates a new CRS derived from the specified one, but with different axis order or unit.
     * This is for implementing the {@link #createSameType(AbstractCS)} method only.
     */
    private DefaultInertialCRS(final DefaultInertialCRS original, final AbstractCS cs) {
        super(original, null, cs);
        datum = original.datum;
    }

    /**
     * Constructs a new coordinate reference system with the same values than the specified one.
     * This copy constructor provides a way to convert an arbitrary implementation into a SIS one
     * or a user-defined one (as a subclass), usually in order to leverage some implementation-specific API.
     *
     * <p>This constructor performs a shallow copy, i.e. the properties are not cloned.</p>
     *
     * @param  crs  the coordinate reference system to copy.
     */
    protected DefaultInertialCRS(final InertialCRS crs) {
        super(crs);
        datum = crs.getDatum();
    }

    /**
     * Returns the GeoAPI interface implemented by this class.
     * The SIS implementation returns {@code InertialCRS.class}.
     * Subclasses implementing a more specific GeoAPI interface shall override this method.
     *
     * @return the coordinate reference system interface implemented by this class.
     */
    @Override
    public Class<? extends InertialCRS> getInterface() {
        return InertialCRS.class;
    }

    /**
     * Returns the datum.
     *
     * @return the datum.
     */
    @Override
    @XmlElement(name = "inertialReferenceFrame", required = true)
    public InertialReferenceFrame getDatum() {
        return datum;
    }

    /**
     * Returns a coordinate reference system of the same type than this CRS but with different axes.
     */
    @Override
    final AbstractCRS createSameType(final AbstractCS cs) {
        return new DefaultInertialCRS(this, cs);
    }

    /**
     * Formats this CRS as a <cite>Well Known Text</cite> {@code InertialCRS[…]} element.
     *
     * @return {@code "InertialCRS"}.
     */
    @Override
    protected String formatTo(final Formatter formatter) {
        WKTUtilities.appendName(this, formatter, null);
        formatter.newLine(); formatter.append(WKTUtilities.toFormattable(getDatum()));
        formatter.newLine(); formatter.append(WKTUtilities.toFormattable(getCoordinateSystem()));
        formatter.newLine();                        // Put ID element on its own line.
        formatter.setInvalidWKT(this, null);        // Because not an ISO 19162 object.
        return WKTKeywords.InertialCRS;
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
     * Constructs a new object in which every attributes are set to a null value.
     * <strong>This is not a valid object.</strong> This constructor is strictly
     * reserved to JAXB, which will assign values to the fields using reflection.
     */
    DefaultInertialCRS() {
        /*
         * The datum and the coordinate system are mandatory for SIS working. We do not verify their presence
         * here because the verification would have to be done in an 'afterMarshal(…)' method and throwing an
         * exception in that method causes the whole unmarshalling to fail.  But the SC_CRS adapter does some
         * verifications.
         */
    }

    /**
     * Invoked by JAXB at unmarshalling time.
     *
     * @see #getDatum()
     */
    private void setDatum(final InertialReferenceFrame value) {
        if (datum == null) {
            datum = value;
        } else {
            ImplementationHelper.propertyAlreadySet(DefaultInertialCRS.class, "setDatum", "geodeticDatum");
        }
    }

    /**
     * Invoked by JAXB at marshalling time.
     * See implementation note in {@link DefaultGeodeticCRS}.
     */
    @XmlElement(namespace=Namespaces.GML, name="ellipsoidalCS") private EllipsoidalCS getEllipsoidalCS() {return getCoordinateSystem(EllipsoidalCS.class);}
    @XmlElement(namespace=Namespaces.GML, name="cartesianCS")   private CartesianCS   getCartesianCS()   {return getCoordinateSystem(CartesianCS  .class);}
    @XmlElement(namespace=Namespaces.GML, name="sphericalCS")   private SphericalCS   getSphericalCS()   {return getCoordinateSystem(SphericalCS  .class);}
    @XmlElement(namespace=Namespaces.GML, name="minkowskiCS")   private MinkowskiCS   getMinkowskiCS()   {return getCoordinateSystem(MinkowskiCS  .class);}

    /**
     * Invoked by JAXB at unmarshalling time.
     *
     * @see #getEllipsoidalCS()
     */
    private void setEllipsoidalCS(final EllipsoidalCS cs) {super.setCoordinateSystem("ellipsoidalCS", cs);}
    private void setCartesianCS  (final CartesianCS   cs) {super.setCoordinateSystem("cartesianCS",   cs);}
    private void setSphericalCS  (final SphericalCS   cs) {super.setCoordinateSystem("sphericalCS",   cs);}
    private void setMinkowskiCS  (final MinkowskiCS   cs) {super.setCoordinateSystem("minkowskiCS",   cs);}

    /**
     * Coordinate systems permitted with this object. This is actually used by the constructor,
     * but defined here for keeping this list close to GML types.
     */
    private static final Class<?>[] PERMITTED_CS_TYPES = {
        EllipsoidalCS.class, CartesianCS.class, SphericalCS.class, MinkowskiCS.class
    };
}
