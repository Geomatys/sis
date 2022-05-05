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
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.metadata.iso.citation.Citations;


/**
 * The provider for <cite>"Mercator Auxiliary Sphere"</cite> projection (defined by ESRI).
 * This is often equivalent to {@link PseudoMercator}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 * @since   1.2
 * @module
 */
@XmlTransient
public final class MercatorAuxiliarySphere extends AbstractMercator {
    /**
     * For cross-version compatibility.
     */
    private static final long serialVersionUID = -8000127893422503988L;

    /**
     * The operation parameter descriptor for the <cite>Auxiliary sphere type</cite> parameter value.
     * Valid values are:
     *
     * <ul>
     *   <li>0 = use semi-major axis or radius of the geographic coordinate system.</li>
     *   <li>1 = use semi-minor axis or radius.</li>
     *   <li>2 = calculate and use authalic radius.</li>
     *   <li>3 = use authalic radius and convert geodetic latitudes to authalic latitudes.</li>
     * </ul>
     *
     * <!-- Generated by ParameterNameTableGenerator -->
     * <table class="sis">
     *   <caption>Parameter names</caption>
     *   <tr><td> ESRI:    </td><td> Auxiliary_Sphere_Type </td></tr>
     * </table>
     * <b>Notes:</b>
     * <ul>
     *   <li>Value domain: [0…3]</li>
     *   <li>Default value: 0</li>
     * </ul>
     */
    public static final ParameterDescriptor<Integer> AUXILIARY_SPHERE_TYPE;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    private static final ParameterDescriptorGroup PARAMETERS;
    static {
        final ParameterBuilder builder = builder().setCodeSpace(Citations.ESRI, "ESRI");
        AUXILIARY_SPHERE_TYPE = builder.addName("Auxiliary_Sphere_Type").createBounded(0, 3, 0);

        final ParameterDescriptor<?>[] descriptors = toArray(Mercator2SP.PARAMETERS.descriptors(), 1);
        descriptors[descriptors.length - 1] = AUXILIARY_SPHERE_TYPE;
        PARAMETERS = builder.addName("Mercator_Auxiliary_Sphere").createGroupForMapProjection(descriptors);
    }

    /**
     * Constructs a new provider.
     */
    public MercatorAuxiliarySphere() {
        super(PARAMETERS);
    }
}
