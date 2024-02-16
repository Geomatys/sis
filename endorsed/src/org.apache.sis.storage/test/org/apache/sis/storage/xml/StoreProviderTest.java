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
package org.apache.sis.storage.xml;

import java.io.StringReader;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.DataStoreException;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.DependsOn;
import org.apache.sis.test.TestCase;


/**
 * Tests {@link StoreProvider}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
@DependsOn(org.apache.sis.storage.StorageConnectorTest.class)
public final class StoreProviderTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public StoreProviderTest() {
    }

    /**
     * Tests {@link StoreProvider#probeContent(StorageConnector)} method from a {@link java.io.Reader} object.
     *
     * @throws DataStoreException if en error occurred while reading the XML.
     */
    @Test
    public void testProbeContentFromReader() throws DataStoreException {
        final var p = new StoreProvider();
        final var c = new StorageConnector(new StringReader(StoreTest.XML));
        final var r = p.probeContent(c);
        c.closeAllExcept(null);
        assertTrue(r.isSupported());
        assertEquals("application/vnd.iso.19139+xml", r.getMimeType());
    }
}
