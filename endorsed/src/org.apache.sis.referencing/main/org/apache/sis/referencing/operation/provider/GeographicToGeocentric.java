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
package org.apache.sis.referencing.operation.provider;

import java.util.OptionalInt;
import javax.measure.Unit;
import javax.measure.quantity.Length;
import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.referencing.cs.CartesianCS;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.operation.Conversion;
import org.opengis.referencing.operation.MathTransform;
import org.apache.sis.referencing.operation.transform.EllipsoidToCentricTransform;
import org.apache.sis.referencing.operation.transform.DefaultMathTransformFactory;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.parameter.Parameters;
import org.apache.sis.util.privy.Constants;


/**
 * The provider for <q>Geographic/geocentric conversions</q> (EPSG:9602).
 * This provider creates transforms from geographic to geocentric coordinate reference systems.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 *
 * @see GeocentricToGeographic
 */
public final class GeographicToGeocentric extends AbstractProvider {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -5690807111952562344L;

    /**
     * The OGC name used for this operation method. The OGC name is preferred to the EPSG name in Apache SIS
     * implementation because it allows to distinguish between the forward and the inverse conversion.
     */
    public static final String NAME = "Ellipsoid_To_Geocentric";

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    public static final ParameterDescriptorGroup PARAMETERS;
    static {
        PARAMETERS = builder()
                .addIdentifier("9602")
                .addName("Geographic/geocentric conversions")
                .addName(Citations.OGC, NAME)
                .createGroupForMapProjection();
                // Not really a map projection, but we leverage the same axis parameters.
    }

    /**
     * Creates a new provider.
     */
    public GeographicToGeocentric() {
        super(Conversion.class, PARAMETERS,
              EllipsoidalCS.class, true,
              CartesianCS.class, false,
              (byte) 2);
    }

    /**
     * If the user asked for the <q>Geographic/geocentric conversions</q> operation but the parameter types
     * suggest that (s)he intended to convert in the opposite direction, returns the name of operation method to use.
     * We need this check because EPSG defines a single operation method for both {@code "Ellipsoid_To_Geocentric"}
     * and {@code "Geocentric_To_Ellipsoid"} methods.
     *
     * <p><b>Note:</b> we do not define similar method in {@link GeocentricToGeographic} class because the only
     * way to obtain that operation method is to ask explicitly for {@code "Geocentric_To_Ellipsoid"} operation.
     * The ambiguity that we try to resolve here exists only if the user asked for the EPSG:9602 operation,
     * which is defined only in this class.</p>
     *
     * @return {@code "Geocentric_To_Ellipsoid"} if the user apparently wanted to get the inverse of this
     *         {@code "Ellipsoid_To_Geocentric"} operation, or {@code null} if none.
     */
    @Override
    public String resolveAmbiguity(final DefaultMathTransformFactory.Context context) {
        if (context.getSourceCS() instanceof CartesianCS && context.getTargetCS() instanceof EllipsoidalCS) {
            return GeocentricToGeographic.NAME;
        }
        return super.resolveAmbiguity(context);
    }

    /**
     * Creates a transform from the specified group of parameter values.
     *
     * @param  context  the parameter values together with its context.
     * @return the conversion from geographic to geocentric coordinates.
     * @throws FactoryException if an error occurred while creating a transform.
     */
    @Override
    public MathTransform createMathTransform(final Context context) throws FactoryException {
        return create(context, context.getSourceDimensions());
    }

    /**
     * Implementation of {@link #createMathTransform(Context)} shared with {@link GeocentricToGeographic}.
     */
    static MathTransform create(final Context context, final OptionalInt dimension) throws FactoryException {
        final Parameters values = Parameters.castOrWrap(context.getCompletedParameters());
        final ParameterValue<?> semiMajor = values.parameter(Constants.SEMI_MAJOR);
        final Unit<Length> unit = semiMajor.getUnit().asType(Length.class);
        return EllipsoidToCentricTransform.createGeodeticConversion(context.getFactory(), semiMajor.doubleValue(),
                values.parameter(Constants.SEMI_MINOR).doubleValue(unit), unit, dimension.orElse(3) >= 3,
                EllipsoidToCentricTransform.TargetType.CARTESIAN);
    }
}
