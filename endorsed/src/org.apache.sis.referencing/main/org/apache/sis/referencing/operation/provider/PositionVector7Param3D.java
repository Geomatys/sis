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

import jakarta.xml.bind.annotation.XmlTransient;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.referencing.operation.MathTransform;


/**
 * The provider for <q>Position Vector transformation (geog3D domain)</q> (EPSG:1037).
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 */
@XmlTransient
public final class PositionVector7Param3D extends GeocentricAffineBetweenGeographic {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -8909802769265229915L;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    static final ParameterDescriptorGroup PARAMETERS;
    static {
        PARAMETERS = builder()
                .addIdentifier("1037")
                .addName("Position Vector transformation (geog3D domain)")
                .createGroupWithSameParameters(PositionVector7Param2D.PARAMETERS);
        /*
         * NOTE: we omit the "Bursa-Wolf" alias because it is ambiguous, since it can apply
         * to both "Coordinate Frame rotation" and "Position Vector 7-param. transformation"
         * We also omit "Position Vector 7-param. transformation" alias for similar reason.
         */
    }

    /**
     * The canonical instance of this operation method.
     *
     * @see #provider()
     */
    private static final PositionVector7Param3D INSTANCE = new PositionVector7Param3D();

    /**
     * Returns the canonical instance of this operation method.
     * This method is invoked by {@link java.util.ServiceLoader} using reflection.
     *
     * @return the canonical instance of this operation method.
     */
    public static PositionVector7Param3D provider() {
        return INSTANCE;
    }

    /**
     * Creates a new provider.
     *
     * @todo Make this constructor private after we stop class-path support.
     */
    public PositionVector7Param3D() {
        super(Type.SEVEN_PARAM, PARAMETERS, (byte) 3);
    }

    /**
     * Returns the operation method which is the closest match for the given transform.
     * This is an adjustment based on the number of dimensions only, on the assumption
     * that the given transform has been created by this provider or a compatible one.
     */
    @Override
    public AbstractProvider variantFor(final MathTransform transform) {
        return maxDimension(transform) < 3 ? PositionVector7Param2D.provider() : this;
    }
}
