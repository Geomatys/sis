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
package org.apache.sis.xml.bind.referencing;

import jakarta.xml.bind.annotation.XmlElement;
import org.opengis.referencing.cs.LinearCS;
import org.apache.sis.referencing.cs.DefaultLinearCS;
import org.apache.sis.xml.bind.gco.PropertyType;


/**
 * JAXB adapter mapping implementing class to the GeoAPI interface. See
 * package documentation for more information about JAXB and interface.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class CS_LinearCS extends PropertyType<CS_LinearCS, LinearCS> {
    /**
     * Empty constructor for JAXB only.
     */
    public CS_LinearCS() {
    }

    /**
     * Returns the GeoAPI interface which is bound by this adapter.
     * This method is indirectly invoked by the private constructor
     * below, so it shall not depend on the state of this object.
     *
     * @return {@code LinearCS.class}
     */
    @Override
    protected Class<LinearCS> getBoundType() {
        return LinearCS.class;
    }

    /**
     * Constructor for the {@link #wrap} method only.
     */
    private CS_LinearCS(final LinearCS cs) {
        super(cs);
    }

    /**
     * Invoked by {@link PropertyType} at marshalling time for wrapping the given value
     * in a {@code <gml:LinearCS>} XML element.
     *
     * @param  cs  the element to marshal.
     * @return a {@code PropertyType} wrapping the given the element.
     */
    @Override
    protected CS_LinearCS wrap(final LinearCS cs) {
        return new CS_LinearCS(cs);
    }

    /**
     * Invoked by JAXB at marshalling time for getting the actual element to write
     * inside the {@code <gml:LinearCS>} XML element.
     * This is the value or a copy of the value given in argument to the {@code wrap} method.
     *
     * @return the element to be marshalled.
     */
    @XmlElement(name = "LinearCS")
    public DefaultLinearCS getElement() {
        return DefaultLinearCS.castOrCopy(metadata);
    }

    /**
     * Invoked by JAXB at unmarshalling time for storing the result temporarily.
     *
     * @param  cs  the unmarshalled element.
     */
    public void setElement(final DefaultLinearCS cs) {
        metadata = cs;
    }
}
