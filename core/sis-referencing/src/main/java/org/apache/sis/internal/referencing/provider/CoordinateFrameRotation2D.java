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
import org.opengis.parameter.ParameterDescriptorGroup;
import org.apache.sis.metadata.iso.citation.Citations;


/**
 * The provider for <cite>"Coordinate Frame Rotation (geog2D domain)"</cite> (EPSG:9607).
 * This is the same transformation than "{@link PositionVector7Param}"
 * except that the rotation angles have the opposite sign.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 * @version 1.3
 * @since   0.7
 * @module
 */
@XmlTransient
public final class CoordinateFrameRotation2D extends GeocentricAffineBetweenGeographic {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 5513675854809530038L;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    private static final ParameterDescriptorGroup PARAMETERS;
    static {
        PARAMETERS = builder()
                .addIdentifier("9607")
                .addName("Coordinate Frame Rotation (geog2D domain)")
                .addName(Citations.ESRI, "Coordinate_Frame")
                .createGroupWithSameParameters(PositionVector7Param2D.PARAMETERS);
        /*
         * NOTE: we omit the "Bursa-Wolf" alias because it is ambiguous, since it can apply
         * to both "Coordinate Frame Rotation" and "Position Vector 7-param. transformation"
         * We also omit "Coordinate Frame Rotation" alias for similar reason.
         */
    }

    /**
     * Constructs the provider.
     */
    public CoordinateFrameRotation2D() {
        this(null);
    }

    /**
     * Constructs a provider that can be resized.
     */
    CoordinateFrameRotation2D(GeodeticOperation[] redimensioned) {
        super(Type.FRAME_ROTATION, PARAMETERS, 2, 2, redimensioned);
    }

    /**
     * Returns the three-dimensional variant of this operation method.
     */
    @Override
    Class<CoordinateFrameRotation3D> variant3D() {
        return CoordinateFrameRotation3D.class;
    }
}
