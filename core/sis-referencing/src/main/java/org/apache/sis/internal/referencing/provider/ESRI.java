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

import org.opengis.parameter.ParameterDescriptor;
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.util.Static;


/**
 * Constants for projections defined by ESRI but not by EPSG.
 * Also used for some projections not defined by ESRI, but in which we reuse ESRI parameters.
 * A characteristics of ESRI parameters is that they have the same name for all projections
 * (at least all the ones supported by SIS). A similar pattern is observed with OGC parameters,
 * which are close to ESRI ones.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.0
 * @module
 */
final class ESRI extends Static {
    /**
     * The operation parameter descriptor for the <cite>Longitude of origin</cite> (λ₀) parameter value.
     * Valid values range is [-180 … 180]° and default value is 0°.
     */
    static final ParameterDescriptor<Double> CENTRAL_MERIDIAN;

    /**
     * The operation parameter descriptor for the <cite>Latitude of origin</cite> (φ₀) parameter value.
     * Valid values range is (-90 … 90)° and default value is 0°.
     */
    static final ParameterDescriptor<Double> LATITUDE_OF_ORIGIN;

    /**
     * The operation parameter descriptor for the <cite>Latitude of 1st standard parallel</cite> parameter value.
     * Valid values range is [-90 … 90]° and default value is 0°.
     */
    static final ParameterDescriptor<Double> STANDARD_PARALLEL_1;

    /**
     * The operation parameter descriptor for the <cite>Latitude of 2nd standard parallel</cite> parameter value.
     * Valid values range is [-90 … 90]° and default value is 0°.
     */
    static final ParameterDescriptor<Double> STANDARD_PARALLEL_2;

    /**
     * The operation parameter descriptor for the <cite>False easting</cite> (FE) parameter value.
     * Valid values range is unrestricted and default value is 0 metre.
     */
    static final ParameterDescriptor<Double> FALSE_EASTING;

    /**
     * The operation parameter descriptor for the <cite>False northing</cite> (FN) parameter value.
     * Valid values range is unrestricted and default value is 0 metre.
     */
    static final ParameterDescriptor<Double> FALSE_NORTHING;
    static {
        final ParameterBuilder builder = MapProjection.builder();
        CENTRAL_MERIDIAN    = MapProjection.createLongitude(copyNames(builder, Equirectangular.LONGITUDE_OF_ORIGIN));
        LATITUDE_OF_ORIGIN  = MapProjection.createLatitude (copyNames(builder, Equirectangular.LATITUDE_OF_ORIGIN), true);
        STANDARD_PARALLEL_1 = MapProjection.createLatitude (copyNames(builder, LambertConformal2SP.STANDARD_PARALLEL_1), true);
        STANDARD_PARALLEL_2 = MapProjection.createLatitude (copyNames(builder, LambertConformal2SP.STANDARD_PARALLEL_2), true);
        FALSE_EASTING       = MapProjection.createShift    (copyNames(builder, Equirectangular.FALSE_EASTING));
        FALSE_NORTHING      = MapProjection.createShift    (copyNames(builder, Equirectangular.FALSE_NORTHING));
    }

    /**
     * Do not allow instantiation of this class.
     */
    private ESRI() {
    }

    /**
     * Copies the ESRI, OGC and PROJ4 names from the given parameters to the given builder.
     * Those parameters are selected because those authorities use the same names in all projections.
     * The EPSG name is discarded because the name varies depending in the projection, in attempts to
     * describe more precisely what they are for.
     *
     * @param  builder   the builder where to add the names.
     * @param  template  the parameter from which to copy the names and identifiers.
     * @return the given {@code builder}, for method call chaining.
     */
    static ParameterBuilder copyNames(final ParameterBuilder builder, final ParameterDescriptor<Double> template) {
        return builder.addName(MapProjection.sameNameAs(Citations.ESRI,  template))
                      .addName(MapProjection.sameNameAs(Citations.OGC,   template))
                      .addName(MapProjection.sameNameAs(Citations.PROJ4, template));
    }
}
