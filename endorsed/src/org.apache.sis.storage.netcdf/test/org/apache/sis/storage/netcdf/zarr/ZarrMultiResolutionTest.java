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
package org.apache.sis.storage.netcdf.zarr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.storage.netcdf.base.Variable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.sis.storage.netcdf.base.TestCase.createListeners;

/**
 * Tests for Zarr multiscale variable discovery.
 */
public class ZarrMultiResolutionTest {

    @TempDir
    Path tempDir;

    @Test
    public void testGetMultiresolutionVariables() throws IOException, DataStoreException {
        // Create Zarr structure
        Path root = tempDir.resolve("test.zarr");
        Files.createDirectories(root);

        // Root group
        String rootJson = "{"
                + "\"zarr_format\": 3,"
                + "\"node_type\": \"group\""
                + "}";
        Files.writeString(root.resolve("zarr.json"), rootJson);

        // Multiscale group
        Path groupDir = root.resolve("data");
        Files.createDirectories(groupDir);
        String groupJson = "{"
                + "\"zarr_format\": 3,"
                + "\"node_type\": \"group\","
                + "\"attributes\": {"
                + "    \"multiscales\": [{"
                + "        \"name\": \"my_pyramid\","
                + "        \"datasets\": [{"
                + "                \"path\": \"level0\""
                + "            }, {"
                + "                \"path\": \"level1\""
                + "            }],"
                // Note: Our ZarrMultiscale POJO expects "layout" and "asset" keys (from V3 spec
                // draft or OME-Zarr?)
                // Let's check ZarrMultiscale.java structure again.
                // It uses: layout -> List<Level>, Level -> asset.
                // So JSON should match that.
                + "        \"layout\": [{"
                + "             \"asset\": \"level0\""
                + "        }, {"
                + "             \"asset\": \"level1\""
                + "        }]"
                + "    }]"
                + "}"
                + "}";
        Files.writeString(groupDir.resolve("zarr.json"), groupJson);

        // Level 0 array
        createArray(groupDir.resolve("level0"), new int[] { 100, 100 });

        // Level 1 array
        createArray(groupDir.resolve("level1"), new int[] { 50, 50 });

        // Open decoder
        StoreListeners listeners = createListeners();
        ZarrDecoder decoder = new ZarrDecoder(root, GeometryLibrary.JAVA2D, listeners);

        // Test
        Collection<List<Variable>> pyramids = decoder.getMultiresolutionVariables();
        Assertions.assertEquals(1, pyramids.size(), "Should find 1 pyramid");

        List<Variable> pyramid = pyramids.iterator().next();
        Assertions.assertEquals(2, pyramid.size(), "Pyramid should have 2 levels");

        Assertions.assertEquals("level0", pyramid.get(0).getName());
        Assertions.assertEquals("level1", pyramid.get(1).getName());

        decoder.close(null);
    }

    private void createArray(Path arrayDir, int[] shape) throws IOException {
        Files.createDirectories(arrayDir);
        String arrayJson = "{"
                + "\"zarr_format\": 3,"
                + "\"node_type\": \"array\","
                + "\"data_type\": \"int32\","
                + "\"shape\": [" + shape[0] + ", " + shape[1] + "],"
                + "\"chunk_grid\": {"
                + "    \"name\": \"regular\","
                + "    \"configuration\": {"
                + "        \"chunk_shape\": [10, 10]"
                + "    }"
                + "},"
                + "\"chunk_key_encoding\": {"
                + "    \"name\": \"default\","
                + "    \"configuration\": {"
                + "        \"separator\": \"/\""
                + "    }"
                + "},"
                + "\"fill_value\": 0,"
                + "\"codecs\": []"
                + "}";
        Files.writeString(arrayDir.resolve("zarr.json"), arrayJson);
    }
}
