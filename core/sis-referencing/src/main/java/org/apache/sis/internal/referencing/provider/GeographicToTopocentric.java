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

import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.referencing.cs.CartesianCS;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.operation.Conversion;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransformFactory;
import org.opengis.referencing.operation.TransformException;
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.measure.Units;
import org.apache.sis.parameter.Parameters;


/**
 * The provider for the <cite>"Geographic/topocentric conversions"</cite> (EPSG:9837).
 * This operation is implemented using existing {@link MathTransform} implementations;
 * there is no need for a class specifically for this transform.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.3
 * @since   1.3
 * @module
 */
public final class GeographicToTopocentric extends AbstractProvider {
    /**
     * For cross-version compatibility.
     */
    private static final long serialVersionUID = -3829993731324133815L;

    /**
     * The operation parameter descriptor for the <cite>Longitude of topocentric origin</cite> parameter value.
     *
     * <!-- Generated by ParameterNameTableGenerator -->
     * <table class="sis">
     *   <caption>Parameter names</caption>
     *   <tr><td> EPSG:    </td><td> Longitude of topocentric origin </td></tr>
     * </table>
     * <b>Notes:</b>
     * <ul>
     *   <li>Value domain: [-180.0 … 180.0]°</li>
     * </ul>
     */
    static final ParameterDescriptor<Double> ORIGIN_X;

    /**
     * The operation parameter descriptor for the <cite>Latitude of topocentric origin</cite> parameter value.
     *
     * <!-- Generated by ParameterNameTableGenerator -->
     * <table class="sis">
     *   <caption>Parameter names</caption>
     *   <tr><td> EPSG:    </td><td> Latitude of topocentric origin </td></tr>
     * </table>
     * <b>Notes:</b>
     * <ul>
     *   <li>Value domain: [-90.0 … 90.0]°</li>
     * </ul>
     */
    static final ParameterDescriptor<Double> ORIGIN_Y;

    /**
     * The operation parameter descriptor for the <cite>Ellipsoidal height of topocentric origin</cite> parameter value.
     *
     * <!-- Generated by ParameterNameTableGenerator -->
     * <table class="sis">
     *   <caption>Parameter names</caption>
     *   <tr><td> EPSG:    </td><td> Ellipsoidal height of topocentric origin </td></tr>
     * </table>
     */
    static final ParameterDescriptor<Double> ORIGIN_Z;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    private static final ParameterDescriptorGroup PARAMETERS;
    static {
        final ParameterBuilder builder = builder();
        ORIGIN_X = createLongitude(builder
                .addIdentifier("8835")
                .addName("Longitude of topocentric origin"));

        ORIGIN_Y = createLatitude(builder
                .addIdentifier("8834")
                .addName("Latitude of topocentric origin"), true);

        ORIGIN_Z = builder
                .addIdentifier("8836")
                .addName("Ellipsoidal height of topocentric origin")
                .create(0, Units.METRE);

        PARAMETERS = builder
                .addIdentifier("9837")
                .addName("Geographic/topocentric conversions")
                .createGroupForMapProjection(ORIGIN_Y, ORIGIN_X, ORIGIN_Z);
                // Not really a map projection, but we leverage the same axis parameters.
    }

    /**
     * Constructs a provider for the 3-dimensional case.
     * While this operation method looks like a map projection because it has a
     * {@link org.opengis.referencing.crs.GeographicCRS} source and
     * {@link org.opengis.referencing.cs.CartesianCS} destination,
     * it is classified in the "Coordinate Operations other than Map Projections" category in EPSG guidance note.
     */
    public GeographicToTopocentric() {
        super(Conversion.class, PARAMETERS,
              EllipsoidalCS.class, 3, true,
              CartesianCS.class, 3, false);
    }

    /**
     * Creates a transform from the specified group of parameter values.
     * The unit of measurement of input coordinates will be the units of the ellipsoid axes.
     *
     * @param  factory  the factory to use for creating the transform.
     * @param  values   the parameter values that define the transform to create.
     * @return the conversion from geographic to topocentric coordinates.
     * @throws FactoryException if an error occurred while creating a transform.
     */
    @Override
    public MathTransform createMathTransform(final MathTransformFactory factory, final ParameterValueGroup values)
            throws FactoryException
    {
        try {
            return GeocentricToTopocentric.create(factory, Parameters.castOrWrap(values), true);
        } catch (TransformException e) {
            throw new FactoryException(e);
        }
    }
}
