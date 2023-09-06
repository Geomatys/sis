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
package org.apache.sis.referencing.datum;

import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.sis.xml.Namespaces;


/**
 * A triaxial ellipsoid, defined as a separated class because it is not a GML 3.2 class.
 * This is used only for XML (un)marshalling.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
@XmlType(name = "TriaxialEllipsoidType", namespace = Namespaces.GSP)
@XmlRootElement(name = "TriaxialEllipsoid", namespace = Namespaces.GSP)
final class TriaxialEllipsoid extends DefaultEllipsoid {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 4631884216927660543L;

    /**
     * Constructs a new object in which every attributes are set to a null value.
     * <strong>This is not a valid object.</strong> This constructor is strictly
     * reserved to JAXB, which will assign values to the fields using reflection.
     */
    public TriaxialEllipsoid() {
    }
}
