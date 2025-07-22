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
                DataType.DOUBLE, new int[]{Math.toIntExact(xDim.length())}, new int[]{Math.toIntExact(xDim.length())}, xData, null);
        Variable yVar = encoder.buildVariable("y", new Dimension[]{yDim}, Map.of("test_attribute", "y attribute"),
                DataType.DOUBLE, new int[]{Math.toIntExact(yDim.length())}, new int[]{Math.toIntExact(yDim.length())}, yData, null);

        float[] data = new float[]{
                1.0f, 2.0f, 3.0f, 4.0f, 5.0f,
                6.0f, 7.0f, 8.0f, 9.0f, 10.0f,
                11.0f, 12.0f, 13.0f, 14.0f, 15.0f
        };

        Variable dataVar = encoder.buildVariable("data", new Dimension[]{yDim, xDim}, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{Math.toIntExact(yDim.length()), Math.toIntExact(xDim.length())},
                new int[]{3, 3}, data, null);

        encoder.writeVariables(List.of(xVar, yVar, dataVar));

        ZarrDecoder decoder = (ZarrDecoder) ZarrDecoderTest.createZarrDecoder(rootWrite);
        assertEquals(3,    decoder.getVariables().length);

        Variable var = decoder.getVariables()[0];
        assertEquals("data", var.getName());
        assertEquals(2, var.getGridDimensions().size());
        assertEquals("x", var.getGridDimensions().getFirst().getName());
        assertEquals("y", var.getGridDimensions().getLast().getName());
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
                DataType.STRING, new int[]{Math.toIntExact(sDim.length())}, new int[]{Math.toIntExact(sDim.length())}, sData, null);

        float[] data = new float[]{
                1.0f, 2.0f, 3.0f, 4.0f, 5.0f
        };

        Variable dataVar = encoder.buildVariable("data", new Dimension[]{sDim}, Map.of("test_attribute", "data attribute"),
                DataType.FLOAT, new int[]{Math.toIntExact(sDim.length())},
                new int[]{5}, data, null);

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
}
