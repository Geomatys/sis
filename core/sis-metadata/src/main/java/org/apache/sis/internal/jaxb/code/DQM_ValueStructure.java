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
package org.apache.sis.internal.jaxb.code;

import javax.xml.bind.annotation.XmlElement;
import org.opengis.metadata.quality.ValueStructure;
import org.apache.sis.internal.jaxb.cat.CodeListAdapter;
import org.apache.sis.internal.jaxb.cat.CodeListUID;
import org.apache.sis.xml.Namespaces;


/**
 * JAXB adapter for {@link ValueStructure}
 * in order to wrap the value in an XML element as specified by ISO 19115-3 standard.
 * See package documentation for more information about the handling of {@code CodeList} in ISO 19115-3.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.3
 * @since   1.3
 * @module
 */
public final class DQM_ValueStructure extends CodeListAdapter<DQM_ValueStructure, ValueStructure> {
    /**
     * Empty constructor for JAXB only.
     */
    public DQM_ValueStructure() {
    }

    /**
     * Creates a new adapter for the given value.
     */
    private DQM_ValueStructure(final CodeListUID value) {
        super(value);
    }

    /**
     * {@inheritDoc}
     *
     * @return the wrapper for the code list value.
     */
    @Override
    protected DQM_ValueStructure wrap(final CodeListUID value) {
        return new DQM_ValueStructure(value);
    }

    /**
     * {@inheritDoc}
     *
     * @return the code list class.
     */
    @Override
    protected Class<ValueStructure> getCodeListClass() {
        return ValueStructure.class;
    }

    /**
     * Invoked by JAXB on marshalling.
     *
     * @return the value to be marshalled.
     */
    @Override
    @XmlElement(name = "DQM_ValueStructure", namespace = Namespaces.DQM)
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