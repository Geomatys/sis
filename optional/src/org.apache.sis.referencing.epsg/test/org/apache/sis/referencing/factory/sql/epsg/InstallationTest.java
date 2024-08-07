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
package org.apache.sis.referencing.factory.sql.epsg;

import java.io.IOException;
import org.apache.sis.referencing.factory.sql.EPSGInstallerTest;
import org.apache.sis.referencing.factory.sql.InstallationScriptProvider;

// Test dependencies
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/**
 * Tests the creation of the EPSG database.
 * This test requires that the {@code optional/(…snip…)/referencing/factory/sql/epsg/} directory
 * contains links to the non-free {@code Tables.sql}, {@code Data.sql} and {@code FKeys.sql} files.
 * See the {@code README.md} file in that directory for more information.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class InstallationTest extends EPSGInstallerTest {
    /**
     * Creates a new test case.
     */
    public InstallationTest() {
    }

    /**
     * Returns the SQL scripts needed for testing the database creation,
     * or skip the JUnit test if those scripts are not found.
     *
     * @return provider of SQL scripts to execute for building the EPSG geodetic dataset.
     * @throws IOException if en I/O operation was required and failed.
     */
    @Override
    protected InstallationScriptProvider getScripts() throws IOException {
        assumeTrue(ScriptProvider.class.getResource("LICENSE.txt") != null,
                "EPSG resources not found. See `README.md` for manual installation.");
        return new ScriptProvider();
    }
}
