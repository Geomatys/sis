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
package org.apache.sis.storage.netcdf;

import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.storage.Resource;
import org.apache.sis.storage.netcdf.base.RasterResource;
import org.apache.sis.storage.netcdf.zarr.ZarrEncoderTest;
import org.opengis.metadata.Metadata;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.system.Loggers;
import org.apache.sis.util.Version;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestCaseWithLogs;

// Specific to the geoapi-3.1 and geoapi-4.0 branches:
import org.opengis.test.dataset.TestData;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;


/**
 * Tests {@link NetcdfStore}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Quentin Bialota (Geomatys)
 */
public final class NetcdfStoreTest extends TestCaseWithLogs {
    /**
     * Creates a new test case.
     */
    public NetcdfStoreTest() {
        super(Loggers.CRS_FACTORY);
    }

    /**
     * Returns a new netCDF store to test.
     *
     * @param  dataset the name of the datastore to load.
     * @throws DataStoreException if an error occurred while reading the netCDF file.
     */
    private static NetcdfStore create(final TestData dataset) throws DataStoreException {
        return new NetcdfStore(null, new StorageConnector(dataset.location()));
    }

    /**
     * Returns a new netCDF store to test.
     *
     * @param datasetPath the path to the dataset to load.
     * @throws DataStoreException if an error occurred while reading the netCDF or Zarr file.
     */
    private static NetcdfStore create(final Path datasetPath) throws DataStoreException {
        return new NetcdfStore(null, new StorageConnector(datasetPath));
    }

    /**
     * Tests {@link NetcdfStore#getMetadata()}.
     *
     * @throws DataStoreException if an error occurred while reading the netCDF file.
     */
    @Test
    public void testGetMetadata() throws DataStoreException {
        final Metadata metadata;
        try (NetcdfStore store = create(TestData.NETCDF_2D_GEOGRAPHIC)) {
            metadata = store.getMetadata();
            assertSame(metadata, store.getMetadata(), "Should be cached.");
        }
        MetadataReaderTest.compareToExpected(metadata, false).assertMetadataEquals();
        loggings.skipNextLogIfContains("EPSG:4019");        // Deprecated EPSG code.
        loggings.assertNoUnexpectedLog();
    }

    /**
     * Tests {@link NetcdfStore#getConventionVersion()}.
     *
     * @throws DataStoreException if an error occurred while reading the netCDF file.
     */
    @Test
    public void testGetConventionVersion() throws DataStoreException {
        final Version version;
        try (NetcdfStore store = create(TestData.NETCDF_2D_GEOGRAPHIC)) {
            version = store.getConventionVersion();
        }
        assertEquals(1, version.getMajor());
        assertEquals(4, version.getMinor());
        loggings.assertNoUnexpectedLog();
    }

    /**
     * Tests {@link NetcdfStore#getMetadata()} with a zarr file.
     *
     * @throws DataStoreException if an error occurred while reading the zarr file.
     * @throws URISyntaxException if an error occurred while getting the test resource.
     */
    @Test
    public void testGetZarrMetadata() throws DataStoreException, URISyntaxException {
        Path root = Paths.get(NetcdfStoreProviderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr").toURI());
        final Metadata metadata;
        try (NetcdfStore store = create(root)) {
            metadata = store.getMetadata();
            assertSame(metadata, store.getMetadata(), "Should be cached.");
        }
        MetadataReaderTest.compareZarrToExpected(metadata).assertMetadataEquals();
        loggings.assertNoUnexpectedLog();
    }

    /**
     * Tests {@link NetcdfStore#getConventionVersion()} with a zarr file.
     *
     * @throws DataStoreException if an error occurred while reading the zarr file.
     * @throws URISyntaxException if an error occurred while getting the test resource.
     */
    @Test
    public void testGetZarrConventionVersion() throws DataStoreException, URISyntaxException {
        Path root = Paths.get(NetcdfStoreProviderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr").toURI());
        try (NetcdfStore store = create(root)) {
            assertNull(store.getConventionVersion());
        }
        loggings.assertNoUnexpectedLog();
    }

    @Test
    public void testReadZippedZarr() throws DataStoreException, URISyntaxException {
        Path root = Paths.get(NetcdfStoreProviderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr/geo.zarr.zip").toURI());
        try (NetcdfStore store = create(root)) {
            Collection<Resource> resources = store.components();
            assertFalse(resources.isEmpty(), "Zarr store should not be empty");

            RasterResource airResource = (RasterResource) resources.stream().toList().get(0);
            assertNotNull(airResource, "Should contain a raster resource.");

            GridGeometry gg = airResource.getGridGeometry();
            assertNotNull(gg, "Should have a grid geometry.");
            assertEquals(3, gg.getExtent().getDimension(), "Grid dimension.");
        }
    }

    /**
     * Tests reading a zarr file and writing it back.
     *
     * @throws DataStoreException if an error occurred while reading or writing the zarr file.
     * @throws URISyntaxException if an I/O error occurred.
     */
    @Test
    public void testReadWriteZarr() throws DataStoreException, URISyntaxException, IOException {
        Path root = Paths.get(NetcdfStoreProviderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr/geo.zarr").toURI());
        Path rootWrite = Paths.get(ZarrEncoderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr").toURI());
        rootWrite = rootWrite.resolve("write_geo.zarr");

        if (Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
        Files.createDirectories(rootWrite);

        NetcdfStore readStore = new NetcdfStore(null, new StorageConnector(root));
        NetcdfStore writeStore = new NetcdfStore(null, new StorageConnector(rootWrite));

        Collection<Resource> resourcesList = readStore.components();

        resourcesList.forEach(resource -> {
            try {
                writeStore.add(resource);
            } catch (DataStoreException e) {
                throw new RuntimeException(e);
            }
        });

        readStore.close();
        writeStore.close();

        // Verification: open output store and check resource count
        try (NetcdfStore checkWriteStore = new NetcdfStore(null, new StorageConnector(rootWrite))) {
            Collection<Resource> writtenResources = checkWriteStore.components();
            assertFalse(writtenResources.isEmpty(), "Output Zarr store should not be empty");

            RasterResource airResource = (RasterResource) writtenResources.stream().toList().getFirst();
            assertNotNull(airResource, "Should contain a raster resource.");

            GridGeometry gg = airResource.getGridGeometry();
            assertNotNull(gg, "Should have a grid geometry.");
            assertEquals(3, gg.getExtent().getDimension(), "Grid dimension.");
        }

        // Cleanup after test
        if (rootWrite != null && Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
    }

    // Helper: Recursively delete a folder
    public static void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(path)) {
                for (Path child : ds) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
