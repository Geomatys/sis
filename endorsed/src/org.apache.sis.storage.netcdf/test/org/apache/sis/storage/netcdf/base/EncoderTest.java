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
package org.apache.sis.storage.netcdf.base;

import org.apache.sis.storage.DataStoreException;
import org.junit.jupiter.api.Test;
import org.opengis.test.dataset.TestData;

import java.io.IOException;
import java.time.Instant;

import static org.apache.sis.storage.netcdf.AttributeNames.CONTRIBUTOR;
import static org.apache.sis.storage.netcdf.AttributeNames.CREATOR;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_CREATED;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_ISSUED;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_MODIFIED;
import static org.apache.sis.storage.netcdf.AttributeNames.LATITUDE;
import static org.apache.sis.storage.netcdf.AttributeNames.LONGITUDE;
import static org.apache.sis.storage.netcdf.AttributeNames.SUMMARY;
import static org.apache.sis.storage.netcdf.AttributeNames.TITLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


/**
 * Tests the {@link Encoder} implementation.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public class EncoderTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public EncoderTest() {
    }
}
