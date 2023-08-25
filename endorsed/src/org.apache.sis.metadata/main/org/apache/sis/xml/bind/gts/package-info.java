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

/**
 * Temporary placeholder for the Temporal Schema JAXB adapters.
 *
 * @author  Guilhem Legal (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Cullen Rombach (Image Matters)
 * @version 1.4
 * @since   0.3
 */
@XmlSchema(elementFormDefault = XmlNsForm.QUALIFIED, namespace = LegacyNamespaces.GTS, xmlns = {
    @XmlNs(prefix = "gts", namespaceURI = LegacyNamespaces.GTS),
    @XmlNs(prefix = "gco", namespaceURI = Namespaces.GCO)
})
@XmlAccessorType(XmlAccessType.NONE)
package org.apache.sis.xml.bind.gts;

import jakarta.xml.bind.annotation.XmlNs;
import jakarta.xml.bind.annotation.XmlNsForm;
import jakarta.xml.bind.annotation.XmlSchema;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import org.apache.sis.xml.Namespaces;
import org.apache.sis.xml.util.LegacyNamespaces;
