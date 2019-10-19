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
package org.apache.sis.internal.referencing.provider;

import javax.xml.bind.annotation.XmlTransient;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.parameter.ParameterNotFoundException;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.parameter.Parameters;
import org.apache.sis.referencing.operation.projection.NormalizedProjection;
import org.apache.sis.util.iso.ResourceInternationalString;
import org.apache.sis.internal.util.Constants;
import org.apache.sis.measure.Units;


/**
 * The provider for <cite>"Satellite-Tracking"</cite> projections.
 * We are not aware of authoritative source for parameter definitions, except the Snyder book.
 * See {@link org.apache.sis.referencing.operation.projection.SatelliteTracking here for more
 * details}.
 *
 * @author  Matthieu Bastianelli (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
@XmlTransient
public class SatelliteTracking extends MapProjection {
    /**
     * For cross-version compatibility.
     */
    private static final long serialVersionUID = -2406880538621713953L;

    /**
     * The operation parameter descriptor for the <cite>central meridian</cite> (λ₀) parameter value.
     * Valid values range is [-180 … 180]° and default value is 0°.
     */
    public static final ParameterDescriptor<Double> CENTRAL_MERIDIAN = ESRI.CENTRAL_MERIDIAN;

    /**
     * Latitude crossing the central meridian at the desired origin of rectangular coordinates.
     */
    public static final ParameterDescriptor<Double> LATITUDE_OF_ORIGIN = ESRI.LATITUDE_OF_ORIGIN;;

    /**
     * The operation parameter descriptor for the <cite>Latitude of 1st standard parallel</cite>.
     * For conical satellite-tracking projection, this is the first parallel of conformality with true scale.
     * Valid values range is [-90 … 90]° and default value is 0°.
     */
    public static final ParameterDescriptor<Double> STANDARD_PARALLEL_1 = ESRI.STANDARD_PARALLEL_1;

    /**
     * The operation parameter descriptor for the <cite>second parallel of conformality but without true scale</cite>
     * parameter value for conic projection. Valid values range is [-90 … 90]° and default value is the opposite value
     * given to the {@link #STANDARD_PARALLEL_1} parameter.
     */
    public static final ParameterDescriptor<Double> STANDARD_PARALLEL_2 = ESRI.STANDARD_PARALLEL_2;

    /**
     * The operation parameter descriptor for the angle of inclination between the plane of the Earth's
     * Equator and the plane of the satellite orbit. It is measured counterclockwise from the Equator to
     * the orbit plane at the ascending node. Examples: 99.092° for Landsat 1, 2 and 3.
     */
    public static final ParameterDescriptor<Double> SATELLITE_ORBIT_INCLINATION;

    /**
     * The operation parameter descriptor for the time required for revolution of the satellite.
     * Examples: 103.267 minutes for Landsat 1, 2 and 3; 98.884 minutes for Landsat 4 and 5.
     */
    public static final ParameterDescriptor<Double> SATELLITE_ORBITAL_PERIOD;

    /**
     * The operation parameter descriptor for the length of Earth's rotation with respect to the
     * precessed ascending node. The ascending node is the point on the satellite orbit at which
     * the satellite crosses the Earth's equatorial plane in a northerly direction.
     * For Landsat (Sun-synchronous satellite orbit), this is 1440 minutes.
     */
    public static final ParameterDescriptor<Double> ASCENDING_NODE_PERIOD;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    static final ParameterDescriptorGroup PARAMETERS;
    static {
        final ParameterBuilder builder = builder().setCodeSpace(Citations.SIS, Constants.SIS);
        SATELLITE_ORBIT_INCLINATION = setNameAndDescription(builder, "satellite_orbit_inclination").create(0, Units.DEGREE);
        SATELLITE_ORBITAL_PERIOD    = setNameAndDescription(builder, "satellite_orbital_period").createStrictlyPositive(Double.NaN, Units.DAY);
        ASCENDING_NODE_PERIOD       = setNameAndDescription(builder, "ascending_node_period")   .createStrictlyPositive(Double.NaN, Units.DAY);
        PARAMETERS = builder.addName("Satellite-Tracking").createGroupForMapProjection(
                LATITUDE_OF_ORIGIN,
                CENTRAL_MERIDIAN,
                STANDARD_PARALLEL_1,
                STANDARD_PARALLEL_2,
                SATELLITE_ORBIT_INCLINATION,
                SATELLITE_ORBITAL_PERIOD,
                ASCENDING_NODE_PERIOD);
    }

    /**
     * Constructs a new provider.
     */
    public SatelliteTracking() {
        super(PARAMETERS);
    }

    /**
     * Sets the parameter name in the given builder, together with a description created from
     * the resource bundle in this package. The resource key is the same as the parameter name.
     *
     * @param  builder  the builder where to set the parameter name and description.
     * @param  name     the parameter name, also used as resource key.
     * @return the builder, for method calls chaining.
     */
    private static ParameterBuilder setNameAndDescription(final ParameterBuilder builder, final String name) {
        return builder.addName(name).setDescription(new ResourceInternationalString(
                "org.apache.sis.internal.referencing.provider.Descriptions", name));
    }

    /**
     * {@inheritDoc}
     *
     * @return the map projection created from the given parameter values.
     */
    @Override
    protected NormalizedProjection createProjection(final Parameters parameters) throws ParameterNotFoundException {
        return new org.apache.sis.referencing.operation.projection.SatelliteTracking(this, parameters);
    }
}
