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

import org.apache.sis.math.Vector;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.storage.netcdf.base.Decoder;
import org.apache.sis.storage.netcdf.base.Dimension;
import org.apache.sis.storage.netcdf.base.Variable;
import org.apache.sis.storage.netcdf.base.VariableRole;
import org.apache.sis.storage.netcdf.base.VariableTest;
import org.junit.jupiter.api.Test;
import org.opengis.test.dataset.TestData;

import java.io.IOException;
import java.util.List;

import static org.apache.sis.storage.netcdf.base.VariableTest.assertBasicPropertiesEqual;
import static org.apache.sis.storage.netcdf.base.VariableTest.assertSingletonEquals;
import static org.apache.sis.storage.netcdf.base.VariableTest.lengths;
import static org.apache.sis.storage.netcdf.base.VariableTest.names;
import static org.apache.sis.storage.netcdf.zarr.ZarrDecoderTest.createZarrDecoder;
import static org.apache.sis.storage.netcdf.zarr.ZarrDecoderTest.getPath;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;


/**
 * Tests the {@link VariableInfo} implementation.
 * passed.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public final class VariableInfoTest {
    /**
     * Creates a new test.
     */
    public VariableInfoTest() {
    }

    /**
     * Creates a new decoder for the specified dataset.
     *
     * @return the decoder for the specified dataset.
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    protected Decoder createDecoder(final TestData file) throws IOException, DataStoreException {
        return createZarrDecoder(file.file().toPath());
    }

    /**
     * Tests the basic properties of all variables.
     * The tested methods are:
     *
     * <ul>
     *   <li>{@link Variable#getName()}</li>
     *   <li>{@link Variable#getDescription()}</li>
     *   <li>{@link Variable#getDataType()}</li>
     *   <li>{@link Variable#getGridDimensions()} length</li>
     *   <li>{@link Variable#getRole()}</li>
     * </ul>
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testBasicProperties() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));

        assertBasicPropertiesEqual(new Object[] {
                // __name______________description_____________________datatype_______dim__role
                "air", null,                             DataType.DOUBLE,   2, VariableRole.COVERAGE,
                "cell", null,                            DataType.UINT64,   1, VariableRole.AXIS,
                "lat", null,                             DataType.DOUBLE,    2, VariableRole.COVERAGE,
                "lon", null,                             DataType.DOUBLE,    2, VariableRole.COVERAGE,
                "time", null,                            DataType.INT64,    1, VariableRole.AXIS,
        }, decoder.getVariables());

        final Decoder geoDecoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/geo.zarr"));

        assertBasicPropertiesEqual(new Object[] {
                // __name______________description_____________________datatype_______dim__role
                "air", null,                             DataType.DOUBLE,   3, VariableRole.COVERAGE,
                "lat", null,                             DataType.DOUBLE,   1, VariableRole.AXIS,
                "lon", null,                             DataType.DOUBLE,   1, VariableRole.AXIS,
                "time", null,                            DataType.INT64,    1, VariableRole.AXIS,
        }, geoDecoder.getVariables());
    }

    /**
     * Tests {@link Variable#getGridDimensions()} on a simple two-dimensional dataset.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testGridRange2D() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/basic.zarr"));

        final Variable variable = decoder.getVariables()[0];
        assertEquals("bar", variable.getName());

        final List<Dimension> dimensions = variable.getGridDimensions();
        assertArrayEquals(new String[] {"y", "x"}, names(dimensions));
        assertArrayEquals(new long[] {10, 10}, lengths(dimensions));
    }

    /**
     * Tests {@link Variable#getAttributeValue(String)} and related methods.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testGetAttributes() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));

        final Variable[] variables = decoder.getVariables();
        Variable variable = variables[0];
        assertEquals("air", variable.getName());
        assertSingletonEquals(variable, "_FillValue", 0.0d);
    }

    /**
     * Tests {@link Variable#read()} on a one-dimensional variable.
     *
     * @throws IOException if an error occurred while reading the netCDF file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testRead1D() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));

        final Variable variable = decoder.getVariables()[1];
        assertEquals("cell", variable.getName());
        final Vector data = variable.read();
        assertSame(data, variable.readAnyType());
        assertEquals(Long.class, data.getElementType());
        final int length = data.size();
        assertEquals(60, length);
    }
}
