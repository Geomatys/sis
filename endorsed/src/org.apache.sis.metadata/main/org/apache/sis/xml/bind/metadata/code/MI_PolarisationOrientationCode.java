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
package org.apache.sis.xml.bind.metadata.code;

import jakarta.xml.bind.annotation.XmlElement;
import org.apache.sis.xml.Namespaces;
import org.apache.sis.xml.bind.cat.CodeListAdapter;
import org.apache.sis.xml.bind.cat.CodeListUID;

// Specific to the main and geoapi-3.1 branches:
import org.opengis.metadata.content.PolarizationOrientation;


/**
 * JAXB adapter for {@link PolarizationOrientation}
 * in order to wrap the value in an XML element as specified by ISO 19115-3 standard.
 * See package documentation for more information about the handling of {@code CodeList} in ISO 19115-3.
 *
 * @author  Cédric Briançon (Geomatys)
 * @author  Guilhem Legal (Geomatys)
 * @version 1.4
 *
 * @see <a href="https://issues.apache.org/jira/browse/SIS-398">SIS-398</a>
 *
 * @since 0.3
 */
public final class MI_PolarisationOrientationCode
        extends CodeListAdapter<MI_PolarisationOrientationCode, PolarizationOrientation>
{
    /**
     * Empty constructor for JAXB only.
     */
    public MI_PolarisationOrientationCode() {
    }

    /**
     * Creates a new adapter for the given value.
     */
    private MI_PolarisationOrientationCode(final CodeListUID value) {
        super(value);
    }

    /**
     * {@inheritDoc}
     *
     * @return the wrapper for the code list value.
     */
    @Override
    protected MI_PolarisationOrientationCode wrap(final CodeListUID value) {
        return new MI_PolarisationOrientationCode(value);
    }

    /**
     * {@inheritDoc}
     *
     * @return the code list class.
     */
    @Override
    protected Class<PolarizationOrientation> getCodeListClass() {
        return PolarizationOrientation.class;
    }

    /**
     * Invoked by JAXB on marshalling.
     *
     * @return the value to be marshalled.
     */
    @Override
    @XmlElement(name = "MI_PolarisationOrientationCode", namespace = Namespaces.MRC)
    public CodeListUID getElement() {
        return identifier;
    }

    /**
     * Invoked by JAXB on unmarshalling.
     *
     * @param  value  the unmarshalled value.
     */
    public void setElement(final CodeListUID value) {
        identifier = value;
    }
}
