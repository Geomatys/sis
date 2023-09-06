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
package org.apache.sis.referencing.crs;

import java.io.InputStream;
import jakarta.xml.bind.JAXBException;

// Test dependencies
import org.junit.jupiter.api.Test;
import org.opengis.test.Validators;
import org.apache.sis.xml.test.TestCase;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Tests the {@link DefaultInertialCRS} class.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class DefaultInertialCRSTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public DefaultInertialCRSTest() {
    }

    /**
     * Opens the stream to the XML file in this package containing an inertial CRS definition.
     *
     * @return stream opened on the XML document to use for testing purpose.
     */
    private static InputStream openTestFile() {
        // Call to `getResourceAsStream(…)` is caller sensitive: it must be in the same module.
        return DefaultInertialCRSTest.class.getResourceAsStream("InertialCRS.xml");
    }

    /**
     * Tests (un)marshalling of an inertial coordinate reference system.
     *
     * @throws JAXBException if an error occurred during unmarshalling.
     */
    @Test
    public void testXML() throws JAXBException {
        final DefaultInertialCRS crs = unmarshalFile(DefaultInertialCRS.class, openTestFile());
        Validators.validate(crs);
        assertEquals("Cartesian 3D CS (km).", crs.getCoordinateSystem().getName().getCode(), "coordinateSystem");
        assertEquals("Dimorphos inertial reference frame", crs.getDatum().getName().getCode(), "datum");
    }
}
