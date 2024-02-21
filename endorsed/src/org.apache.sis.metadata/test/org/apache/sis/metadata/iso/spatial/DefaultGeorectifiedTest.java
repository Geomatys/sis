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
package org.apache.sis.metadata.iso.spatial;

import org.opengis.util.InternationalString;
import org.apache.sis.util.SimpleInternationalString;
import org.apache.sis.xml.bind.Context;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestCaseWithLogs;


/**
 * Tests {@link DefaultGeorectified}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class DefaultGeorectifiedTest extends TestCaseWithLogs {
    /**
     * Creates a new test case.
     */
    public DefaultGeorectifiedTest() {
        super(Context.LOGGER);
    }

    /**
     * Tests {@link DefaultGeorectified#isCheckPointAvailable()} and
     * {@link DefaultGeorectified#setCheckPointAvailable(boolean)}.
     */
    @Test
    public void testCheckPointAvailable() {
        final DefaultGeorectified metadata = new DefaultGeorectified();
        final InternationalString description = new SimpleInternationalString("A check point description.");
        assertFalse(metadata.isCheckPointAvailable());

        // Setting the description shall set automatically the availability.
        metadata.setCheckPointDescription(description);
        assertTrue(metadata.isCheckPointAvailable());
        loggings.assertNoUnexpectedLog();

        // Setting the availability flag shall hide the description and logs a message.
        metadata.setCheckPointAvailable(false);
        assertNull(metadata.getCheckPointDescription());
        loggings.assertNextLogContains("checkPointDescription", "checkPointAvailability");
        loggings.assertNoUnexpectedLog();

        // Setting the availability flag shall bring back the description.
        metadata.setCheckPointAvailable(true);
        assertSame(description, metadata.getCheckPointDescription());
        loggings.assertNoUnexpectedLog();
    }
}
