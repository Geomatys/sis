package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.math.Vector;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.storage.netcdf.base.Dimension;
import org.apache.sis.storage.netcdf.base.Encoder;
import org.apache.sis.storage.netcdf.base.EncoderTest;
import org.apache.sis.storage.netcdf.base.Variable;
import org.junit.jupiter.api.Test;
import org.opengis.test.dataset.TestData;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.sis.storage.netcdf.NetcdfStoreTest.deleteRecursively;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link ZarrEncoder} implementation.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public class ZarrEncoderTest extends EncoderTest {
    /**
     * Creates a new test case.
     */
    public ZarrEncoderTest() {
    }

    /**
     * Creates a new encoder for the specified dataset.
     *
     * @return the encoder for the specified dataset.
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    protected Encoder createEncoder(final TestData file) throws IOException, DataStoreException {
        return createZarrEncoder(file.file().toPath());
    }

    public static Path getPath(String filename) throws DataStoreException, IOException {
        try {
            return Paths.get(ZarrEncoderTest.class.getResource(filename).toURI());
        } catch (URISyntaxException e) {
            throw new DataStoreException(e);
        }
    }

    public static ZarrEncoder createZarrEncoder(final Path file) throws IOException, DataStoreException {
        return new ZarrEncoder(file, new int[]{5,5}, 0, GeometryLibrary.JAVA2D, createListeners());
    }

    @Test
    public void testWriteCustomData() throws DataStoreException, IOException, URISyntaxException {
        Path rootWrite = Paths.get(ZarrEncoderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr").toURI());
        rootWrite = rootWrite.resolve("write_custom.zarr");

        if (Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
        Files.createDirectories(rootWrite);

        ZarrEncoder encoder = createZarrEncoder(rootWrite);

        Dimension xDim = encoder.buildDimension("x", 5);
        Dimension yDim = encoder.buildDimension("y", 3);

        double[] xData = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
        double[] yData = new double[]{10.0, 20.0, 30.0};

        Variable xVar = encoder.buildVariable("x", new Dimension[]{xDim}, Map.of("test_attribute", "x attribute"),
                DataType.DOUBLE, new int[]{Math.toIntExact(xDim.length())}, new int[]{Math.toIntExact(xDim.length())}, xData, null, null);
        Variable yVar = encoder.buildVariable("y", new Dimension[]{yDim}, Map.of("test_attribute", "y attribute"),
                DataType.DOUBLE, new int[]{Math.toIntExact(yDim.length())}, new int[]{Math.toIntExact(yDim.length())}, yData, null, null);

        float[] data = new float[]{
                1.0f, 2.0f, 3.0f, 4.0f, 5.0f,
                6.0f, 7.0f, 8.0f, 9.0f, 10.0f,
                11.0f, 12.0f, 13.0f, 14.0f, 15.0f
        };

        Variable dataVar = encoder.buildVariable("data", new Dimension[]{yDim, xDim}, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{Math.toIntExact(yDim.length()), Math.toIntExact(xDim.length())},
                new int[]{3, 3}, data, null, null);

        encoder.writeVariables(List.of(xVar, yVar, dataVar));

        ZarrDecoder decoder = (ZarrDecoder) ZarrDecoderTest.createZarrDecoder(rootWrite);
        assertEquals(3,    decoder.getVariables().length);

        Variable var = decoder.getVariables()[0];
        assertEquals("data", var.getName());
        assertEquals(2, var.getGridDimensions().size());
        assertEquals("y", var.getGridDimensions().getFirst().getName());
        assertEquals("x", var.getGridDimensions().getLast().getName());
        assertArrayEquals(data, var.read().floatValues());

        var = decoder.getVariables()[1];
        assertEquals("x", var.getName());
        assertEquals(1, var.getGridDimensions().size());
        assertArrayEquals(xData, var.read().doubleValues());

        var = decoder.getVariables()[2];
        assertEquals("y", var.getName());
        assertArrayEquals(yData, var.read().doubleValues());

        // Cleanup after test
        if (rootWrite != null && Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
    }

    @Test
    public void testWriteStringData() throws DataStoreException, IOException, URISyntaxException {
        Path rootWrite = Paths.get(ZarrEncoderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr").toURI());
        rootWrite = rootWrite.resolve("write_string_custom.zarr");

        if (Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
        Files.createDirectories(rootWrite);

        ZarrEncoder encoder = createZarrEncoder(rootWrite);

        Dimension sDim = encoder.buildDimension("s", 5);

        String[] sData = new String[]{"hello", "world", "this", "is", "zarr_test_string"};

        Variable sVar = encoder.buildVariable("s", new Dimension[]{sDim}, Map.of("test_attribute", "x attribute"),
                DataType.STRING, new int[]{Math.toIntExact(sDim.length())}, new int[]{Math.toIntExact(sDim.length())}, sData, null, null);

        float[] data = new float[]{
                1.0f, 2.0f, 3.0f, 4.0f, 5.0f
        };

        Variable dataVar = encoder.buildVariable("data", new Dimension[]{sDim}, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{Math.toIntExact(sDim.length())},
                new int[]{5}, data, null, null);

        encoder.writeVariables(List.of(sVar, dataVar));

        ZarrDecoder decoder = (ZarrDecoder) ZarrDecoderTest.createZarrDecoder(rootWrite);
        assertEquals(2,    decoder.getVariables().length);

        Vector vector = decoder.getVariables()[1].read();
        String[] strArr = new String[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            strArr[i] = vector.stringValue(i);
        }

        assertEquals(sData.length, strArr.length);
        for (int i = 0; i < sData.length; i++) {
            assertEquals(sData[i], strArr[i]);
        }

        // Cleanup after test
        if (rootWrite != null && Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
    }

    /**
     * Tests writing variables with unnamed dimensions (null entries in dimension_names per Zarr v3 spec).
     * Checks both a fully-unnamed case and a mixed named/unnamed case.
     * Only the writer side is exercised; decoder support is handled separately.
     */
    @Test
    public void testWriteUnnamedDimensions() throws DataStoreException, IOException, URISyntaxException {
        Path rootWrite = Paths.get(ZarrEncoderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr").toURI());
        rootWrite = rootWrite.resolve("write_unnamed_dims.zarr");

        if (Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
        Files.createDirectories(rootWrite);

        ZarrEncoder encoder = createZarrEncoder(rootWrite);

        // Case 1: fully unnamed dimension  →  dimension_names: [null]
        Dimension unnamedDim = encoder.buildDimension(null, 5);
        float[] data1 = new float[]{1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        Variable var1 = encoder.buildVariable("unnamed_1d", new Dimension[]{unnamedDim},
                Map.of(), DataType.FLOAT,
                new int[]{5}, new int[]{5}, data1, null, null);

        // Case 2: mixed named + unnamed  →  dimension_names: ["x", null]
        Dimension xDim      = encoder.buildDimension("x", 3);
        Dimension unnamed2  = encoder.buildDimension(null, 4);
        float[] data2 = new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        Variable var2 = encoder.buildVariable("mixed_2d", new Dimension[]{xDim, unnamed2},
                Map.of(), DataType.FLOAT,
                new int[]{3, 4}, new int[]{3, 4}, data2, null, null);

        encoder.writeVariables(List.of(var1, var2));

        // Verify the written metadata directly (no decoder needed)
        String json1 = Files.readString(rootWrite.resolve("unnamed_1d").resolve("zarr.json"));
        String json2 = Files.readString(rootWrite.resolve("mixed_2d").resolve("zarr.json"));

        // "dimension_names" array must contain JSON null entries
        assertNotNull(json1);
        assertNotNull(json2);

        // Verify dimension_names in metadata objects
        ZarrArrayMetadata meta1 = ((VariableInfo) var1).metadata;
        ZarrArrayMetadata meta2 = ((VariableInfo) var2).metadata;

        assertNotNull(meta1.dimensionNames());
        assertEquals(1, meta1.dimensionNames().length);
        assertNull(meta1.dimensionNames()[0], "Fully unnamed dimension should be null");

        assertNotNull(meta2.dimensionNames());
        assertEquals(2, meta2.dimensionNames().length);
        assertEquals("x",  meta2.dimensionNames()[0], "First dimension should be named 'x'");
        assertNull(meta2.dimensionNames()[1], "Second dimension should be unnamed (null)");

        // Also verify the JSON on disk contains literal null (not the string "null")
        assertTrue(json1.contains("null"),
                "zarr.json for unnamed_1d should contain a null entry in dimension_names");

        // Cleanup
        deleteRecursively(rootWrite);
    }

    @Test
    public void testWriteDggsCustomData() throws DataStoreException, IOException, URISyntaxException {
        Path rootWrite = Paths.get(ZarrEncoderTest.class.getResource("/org/apache/sis/storage/netcdf/resources/zarr").toURI());
        rootWrite = rootWrite.resolve("write_dggs_custom.zarr");

        if (Files.exists(rootWrite)) {
            deleteRecursively(rootWrite);
        }
        Files.createDirectories(rootWrite);

        ZarrEncoder encoder = createZarrEncoder(rootWrite);

        double[] subzoneData_Depth1_zone1 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
        double[] subzoneData_Depth1_zone2 = new double[]{10.0, 20.0, 30.0};

        double[] subzoneData_Depth2_zone1 = new double[]{6.0, 7.0, 8.0, 9.0, 10.0};
        double[] subzoneData_Depth2_zone2 = new double[]{40.0, 50.0, 60.0};

        // depth1 group: zone1 (5 values) and zone2 (3 values), flat chunk key layout
        Map<String, Object> depth1Conf = Map.of("separator", '_', "group", "depth1");
        Map<String, Object> depth2Conf = Map.of("separator", '_', "group", "depth2");

        Variable zone1_depth1_var = encoder.buildVariable("zone1", null, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{subzoneData_Depth1_zone1.length}, new int[]{subzoneData_Depth1_zone1.length}, subzoneData_Depth1_zone1, null, depth1Conf);

        Variable zone2_depth1_var = encoder.buildVariable("zone2", null, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{subzoneData_Depth1_zone2.length}, new int[]{subzoneData_Depth1_zone2.length}, subzoneData_Depth1_zone2, null, depth1Conf);

        Variable zone1_depth2_var = encoder.buildVariable("zone1", null, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{subzoneData_Depth2_zone1.length}, new int[]{subzoneData_Depth2_zone1.length}, subzoneData_Depth2_zone1, null, depth2Conf);

        Variable zone2_depth2_var = encoder.buildVariable("zone2", null, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{subzoneData_Depth2_zone2.length}, new int[]{subzoneData_Depth2_zone2.length}, subzoneData_Depth2_zone2, null, depth2Conf);

        encoder.writeVariables(List.of(zone1_depth1_var, zone2_depth1_var, zone1_depth2_var, zone2_depth2_var));

        // Verify the group directories and zarr.json files exist on disk
        assertTrue(Files.isDirectory(rootWrite.resolve("depth1")),        "depth1 group directory should exist");
        assertTrue(Files.isDirectory(rootWrite.resolve("depth2")),        "depth2 group directory should exist");
        assertTrue(Files.exists(rootWrite.resolve("depth1/zarr.json")),   "depth1 group metadata should exist");
        assertTrue(Files.exists(rootWrite.resolve("depth2/zarr.json")),   "depth2 group metadata should exist");
        assertTrue(Files.isDirectory(rootWrite.resolve("depth1/zone1")),  "depth1/zone1 array directory should exist");
        assertTrue(Files.isDirectory(rootWrite.resolve("depth1/zone2")),  "depth1/zone2 array directory should exist");
        assertTrue(Files.isDirectory(rootWrite.resolve("depth2/zone1")),  "depth2/zone1 array directory should exist");
        assertTrue(Files.isDirectory(rootWrite.resolve("depth2/zone2")),  "depth2/zone2 array directory should exist");
        // Flat chunk key: c_0 (not c/0)
        assertTrue(Files.exists(rootWrite.resolve("depth1/zone1/c_0")),   "depth1/zone1 chunk file c_0 should exist");

        // Cleanup
        deleteRecursively(rootWrite);
    }
}
