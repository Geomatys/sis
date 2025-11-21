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

import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.GridOrientation;
import org.apache.sis.math.Vector;
import org.apache.sis.measure.Units;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.GridCoverageResource;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.storage.netcdf.base.Decoder;
import org.apache.sis.storage.netcdf.base.Dimension;
import org.apache.sis.storage.netcdf.base.Encoder;
import org.apache.sis.storage.netcdf.base.Grid;
import org.apache.sis.storage.netcdf.base.GridAdjustment;
import org.apache.sis.storage.netcdf.base.Variable;
import org.apache.sis.storage.netcdf.classic.ChannelDecoder;
import org.apache.sis.temporal.LenientDateFormat;
import org.apache.sis.util.ArraysExt;
import org.apache.sis.util.CharSequences;
import org.apache.sis.util.Classes;
import org.apache.sis.util.Numbers;
import org.apache.sis.util.resources.Errors;
import ucar.nc2.constants.CDM;
import ucar.nc2.constants.CF;
import ucar.nc2.constants._Coordinate;

import javax.measure.Unit;
import javax.measure.format.MeasurementParseException;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Description of a variable found in a zarr tree structure.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class VariableInfo extends Variable implements Comparable<VariableInfo> {
    /**
     * The names of attributes where to look for the description to be returned by {@link #getDescription()}.
     */
    private static final String[] DESCRIPTION_ATTRIBUTES = {
            CDM.DESCRIPTION,
    };

    /**
     * The names of attributes where to look for the units to be returned by {@link #getUnitsString()}.
     */
    private static final String[] UNITS_ATTRIBUTES = {
            CDM.UNITS,
    };

    /**
     * The variable name.
     *
     * @see #getName()
     */
    private String name;

    /**
     * The dimensions of this variable.
     */
    final DimensionInfo[] dimensions;

    /**
     * The metadata of the array that contains the values of this variable.
     */
    final ZarrArrayMetadata metadata;

    /**
     * The Zarr type of data, or {@code null} if unknown.
     *
     * @see #getDataType()
     */
    private DataType dataType;

    /**
     * The attributes associates to the variable, or an empty map if none.
     * Values can be:
     *
     * <ul>
     *   <li>{@link String} if the attribute contains a single textual value.</li>
     *   <li>{@link Number} if the attribute contains a single numerical value.</li>
     *   <li>{@link Vector} if the attribute contains many numerical values.</li>
     *   <li>{@code String[]} if the attribute is one of predefined attributes
     *       for which many text values are expected (e.g. an enumeration).</li>
     * </ul>
     *
     * If the value is a {@code String}, then leading and trailing spaces and control characters
     * should be trimmed by {@link String#trim()}.
     */
    private final Map<String,Object> attributes;

    /**
     * Names of attributes. Used for quick lookup and iteration.
     */
    private final Set<String> attributeNames;

    /**
     * The grid geometry associated to this variable, computed by {@link ZarrDecoder#getGridCandidates()} when first needed.
     * May stay {@code null} if the variable is not a data cube. We do not need disambiguation between the case where
     * the grid has not yet been computed and the case where the computation has been done with {@code null} result,
     * because {@link #findGrid(GridAdjustment)} should be invoked only once per variable.
     *
     * @see #findGrid(GridAdjustment)
     */
     GridInfo grid;

    /**
     * {@code true} if this variable seems to be a coordinate system axis, as determined by comparing its name
     * with the name of all dimensions in the netCDF file. This information is computed at construction time
     * because requested more than once.
     */
    boolean isCoordinateSystemAxis;

    VariableInfo(final Encoder encoder,
                 final String  name,
                 final DimensionInfo[] dimensions,
                 final Map<String,Object> attributes,
                 final Set<String> attributeNames,
                 final DataType dataType,
                 final ZarrArrayMetadata  metadata,
                 final Object data,
                 final Integer sampleDimensionIndex) throws DataStoreContentException {
        super(encoder);
        this.name = name;
        this.dimensions = dimensions;
        this.metadata = metadata;
        this.attributes = attributes;
        this.attributeNames = attributeNames;
        this.dataType = dataType;

        /*
         * According CF conventions, a variable is considered a coordinate system axis if it has the same name
         * as its dimension. But the "_CoordinateAxisType" attribute is often used for making explicit that a
         * variable is an axis. We check that case before to check variable name.
         */
        if (dimensions.length == 1 || dimensions.length == 2) {
            isCoordinateSystemAxis = (getAxisType() != null);
            if (!isCoordinateSystemAxis && (dimensions.length == 1 || dataType == DataType.CHAR)) {
                /*
                 * If the "_CoordinateAliasForDimension" attribute is defined, then its value will be used
                 * instead of the variable name when determining if the variable is a coordinate system axis.
                 * "_CoordinateVariableAlias" seems to be a legacy attribute name for the same purpose.
                 */
                Object value = getAttributeValue(_Coordinate.AliasForDimension, "_coordinatealiasfordimension");
                if (value == null) {
                    value = getAttributeValue("_CoordinateVariableAlias", "_coordinatevariablealias");
                    if (value == null) {
                        value = name;
                    }
                }
                isCoordinateSystemAxis = dimensions[0].name.equals(value);
            }
        }

        this.setData(data, sampleDimensionIndex);
    }

    /**
     * Creates a new variable.
     *
     * @param decoder the zarr structure where this variable is defined.
     * @param name the variable name.
     * @param dimensions the dimensions of this variable.
     * @param attributes attributes associated to this variable, or an empty map if none.
     * @param attributeNames names of attributes associated to this variable, or an empty set if none.
     * @param dataType the type of data, or {@code null} if unknown.
     * @param metadata zarr metadata node for the array containing this variable.
     */
    VariableInfo( final Decoder            decoder,
                  final String             name,
                  final DimensionInfo[]    dimensions,
                  final Map<String,Object> attributes,
                  final Set<String>        attributeNames,
                        DataType           dataType,
                  final ZarrArrayMetadata  metadata) throws DataStoreContentException {
        super(decoder);

        this.name = name;
        this.dimensions = dimensions;
        this.metadata = metadata;
        this.attributes = attributes;
        this.attributeNames = attributeNames;
        this.dataType = dataType;

        /*
         * According CF conventions, a variable is considered a coordinate system axis if it has the same name
         * as its dimension. But the "_CoordinateAxisType" attribute is often used for making explicit that a
         * variable is an axis. We check that case before to check variable name.
         */
        if (dimensions.length == 1 || dimensions.length == 2) {
            isCoordinateSystemAxis = (getAxisType() != null);
            if (!isCoordinateSystemAxis && (dimensions.length == 1 || dataType == DataType.CHAR)) {
                /*
                 * If the "_CoordinateAliasForDimension" attribute is defined, then its value will be used
                 * instead of the variable name when determining if the variable is a coordinate system axis.
                 * "_CoordinateVariableAlias" seems to be a legacy attribute name for the same purpose.
                 */
                Object value = getAttributeValue(_Coordinate.AliasForDimension, "_coordinatealiasfordimension");
                if (value == null) {
                    value = getAttributeValue("_CoordinateVariableAlias", "_coordinatevariablealias");
                    if (value == null) {
                        value = name;
                    }
                }
                isCoordinateSystemAxis = dimensions[0].name.equals(value);
            }
        }
    }

    /**
     * Returns -1 if the variable / array is located before the other variable in the Zarr tree structure,
     * or +1 if it is located after.
     * If the two variables are located at the same position in the Zarr tree structure,
     * then this method compares the variable names.
     */
    @Override
    public int compareTo(final VariableInfo other) {
        int c = metadata.zarrPath().compareTo(other.metadata.zarrPath());
        if (c == 0) c = name.compareTo(other.name);
        return c;
    }

    /**
     * Returns the name of this variable.
     */
    @Override
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Returns the description of this variable, or {@code null} if none.
     * This method searches for the first attribute named {@code "description"}.
     */
    @Override
    public String getDescription() {
        for (final String attributeName : DESCRIPTION_ATTRIBUTES) {
            final String value = getAttributeAsString(attributeName);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * Returns the units of this variable, or {@code null} if none.
     * This method searches for the first attribute named {@code "units"}.
     */
    @Override
    protected String getUnitsString() {
        for (final String attributeName : UNITS_ATTRIBUTES) {
            final String value = getAttributeAsString(attributeName);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * Parses the given unit symbol and set the {@link #epoch} if the parsed unit is a temporal unit.
     * This method is called by {@link #getUnit()}.
     *
     * @throws MeasurementParseException if the given symbol cannot be parsed.
     */
    @Override
    protected Unit<?> parseUnit(String symbols) {
        final Matcher parts = TIME_UNIT_PATTERN.matcher(symbols);
        DateTimeParseException dateError = null;
        if (parts.matches()) {
            /*
             * If we enter in this block, the unit is of the form "days since 1970-01-01 00:00:00".
             * The TIME_PATTERN splits the string in two parts, "days" and "1970-01-01 00:00:00".
             * The parse method will replace the space between date and time by 'T' letter.
             */
            try {
                epoch = LenientDateFormat.parseInstantUTC(parts.group(2));
            } catch (DateTimeParseException e) {
                dateError = e;
            }
            symbols = parts.group(1);
        }
        /*
         * Parse the unit symbol after removing the "since 1970-01-01 00:00:00" part of the text,
         * even if we failed to parse the date. We need to be tolerant regarding the date because
         * sometimes the text looks like "hours since analysis".
         */
        final Unit<?> unit;
        try {
            unit = Units.valueOf(symbols);
        } catch (MeasurementParseException e) {
            if (dateError != null) {
                e.addSuppressed(dateError);
            }
            throw e;
        }
        /*
         * Log the warning about date format only if the rest of this method succeeded.
         * We report `getUnit()` as the source method because it is the public caller.
         */
        if (dateError != null) {
            error(Variable.class, "getUnit", dateError, Errors.Keys.CanNotAssignUnitToVariable_2, getName(), symbols);
        }
        return unit;
    }

    /**
     * Returns the type of data, or {@code UNKNOWN} if the data type is unknown to this method.
     * If this variable has a {@code "_Unsigned = true"} attribute, then the returned data type
     */
    @Override
    public DataType getDataType() {
        return dataType;
    }

    /**
     * Returns always {@code false} because Zarr variables are never unlimited (not supported by Zarr v3 for now).
     * @return {@code false} always.
     */
    @Override
    protected boolean isUnlimited() {
        return false;
    }

    /**
     * Returns {@code true} if this variable seems to be a coordinate system axis,
     * determined by comparing its name with the name of all dimensions in the Zarr dataset.
     * Also determined by inspection of {@code "coordinates"} attribute on other variables.
     */
    @Override
    protected boolean isCoordinateSystemAxis() {
        return isCoordinateSystemAxis;
    }

    /**
     * Returns the value of the {@code "_CoordinateAxisType"} or {@code "axis"} attribute, or {@code null} if none.
     * Note that a {@code null} value does not mean that this variable is not an axis.
     */
    @Override
    protected String getAxisType() {
        Object value = getAttributeValue(_Coordinate.AxisType, "_coordinateaxistype");
        if (value == null) {
            value = getAttributeValue(CF.AXIS);
        }
        return (value != null) ? value.toString() : null;
    }

    /**
     * Returns the names of variables to use as axes for this variable, or an empty array if none.
     */
    final CharSequence[] getCoordinateVariables() {
        return CharSequences.split(getAttributeAsString(CF.COORDINATES), ' ');
    }

    /**
     * Returns a builder for the grid geometry of this variable, or {@code null} if this variable is not a data cube.
     * The grid geometry builders are opportunistically cached in {@code VariableInfo} instances after they have been
     * computed by {@link ChannelDecoder#getGridCandidates()}. This method delegates to the super-class method only
     * if the grid requires more analysis than the one performed by {@link ChannelDecoder}.
     *
     * @see ChannelDecoder#getGridCandidates()
     */
    @Override
    protected Grid findGrid(final GridAdjustment adjustment) throws IOException, DataStoreException {
        if (grid == null) {
            decoder.getGridCandidates();                      // Force calculation of grid geometries if not already done.
            if (grid == null) {                               // May have been computed as a side-effect of getGridCandidates().
                grid = (GridInfo) super.findGrid(adjustment); // Non-null if grid dimensions are different than this variable.
            }
        }
        return grid;
    }

    /**
     * Returns the number of grid dimensions. This is the size of the {@link #getGridDimensions()} list.
     *
     * @return number of grid dimensions.
     */
    @Override
    public int getNumDimensions() {
        return (dimensions != null) ? dimensions.length : 0;
    }

    /**
     * Returns the dimensions of this variable in the order they are declared in the Zarr dataset.
     * The dimensions are those of the grid, not the dimensions (or axes) of the coordinate system.
     * In ISO 19123 terminology, the {@linkplain Dimension#length() dimension lengths} give the upper
     * corner of the grid envelope plus one. The lower corner is always (0, 0, …, 0).
     *
     * @see #getNumDimensions()
     */
    @Override
    public List<Dimension> getGridDimensions() {
        List<Dimension> result = new ArrayList<>(Arrays.asList(dimensions));
        Collections.reverse(result);
        return result;
    }

    /**
     * Reads a subsampled sub-area of the variable.
     * Multi-dimensional variables are flattened as a one-dimensional array (wrapped in a vector).
     * Array elements are in "natural" order (inverse of netCDF order).
     *
     * @param  area         indices of cell values to read along each dimension, in "natural" order.
     * @param  subsampling  subsampling along each dimension. 1 means no subsampling.
     * @return the data as an array of a Java primitive type.
     * @throws ArithmeticException if the size of the region to read exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     */
    @Override
    public Vector read(final GridExtent area, final long[] subsampling) throws IOException, DataStoreException {
        return Vector.create(readArray(area, subsampling), dataType.isUnsigned);
    }

    /**
     * Reads a subsampled sub-area of the variable and returns them as a list of any object.
     * Elements in the returned list may be {@link Number} or {@link String} instances.
     *
     * @param  area         indices of cell values to read along each dimension, in "natural" order.
     * @param  subsampling  subsampling along each dimension, or {@code null} if none.
     * @return the data as a list of {@link Number} or {@link String} instances.
     */
    @Override
    public List<?> readAnyType(GridExtent area, long[] subsampling) throws IOException, DataStoreException {
        final Object array = readArray(area, subsampling);
        if (dataType == DataType.CHAR && dimensions.length >= STRING_DIMENSION) {
            return createStringList(array, area);
        }
        return Vector.create(array, dataType.isUnsigned);
    }

    /**
     * Reads all the data for this variable and returns them as an array of a Java primitive type.
     * Multi-dimensional variables are flattened as a one-dimensional array (wrapped in a vector).
     * Fill values/missing values are replaced by NaN if {@link #hasRealValues()} is {@code true}.
     *
     * @throws ArithmeticException if the size of the variable exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     *
     * @see #read()
     */
    @Override
    protected Object readFully() throws IOException, DataStoreException {
        return readArray(null, null);
    }

    @Override
    protected String[] createStringArray(Object chars, int count, int length) {
        return new String[0];
    }

    /**
     * Returns a coordinate for this two-dimensional grid coordinate axis.
     * This is (indirectly) a callback method for {@link Grid#getAxes(Decoder)}.
     *
     * @throws ArithmeticException if the axis size exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     */
    @Override
    protected double coordinateForAxis(final int j, final int i) throws IOException, DataStoreException {
        assert j >= 0 && j < dimensions[0].length : j;
        assert i >= 0 && i < dimensions[1].length : i;
        final long n = dimensions[1].length();
        return read().doubleValue(Math.toIntExact(i + n*j));
    }

    /**
     * Reads the data from this variable and returns them as an array of a Java primitive type.
     * Multi-dimensional variables are flattened as a one-dimensional array (wrapped in a vector).
     * Fill values/missing values are replaced by NaN if {@link #hasRealValues()} is {@code true}.
     * Array elements are in "natural" order (inverse of netCDF order).
     *
     * @param  area         indices (in "natural" order) of cell values to read, or {@code null} for whole variable.
     * @param  subsampling  subsampling along each dimension, or {@code null} if none. Ignored if {@code area} is null.
     * @return the data as an array of a Java primitive type.
     * @throws ArithmeticException if the size of the variable exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     *
     * @see #read()
     * @see #read(GridExtent, long[])
     */
    public Object readArray(final GridExtent area, long[] subsampling) throws IOException, DataStoreException {

        //Shape of the global Zarr array (for the whole variable)
        final int[] arrayShape = this.metadata.shape();

        /*
         * Shape of one chunk in the Zarr array
         *
         * Even when a chunk is located at the edge of the array and doesn't fully fit within the array bounds,
         * the chunking shape you specify (e.g., (5, 5)) is still valid.
         *
         * Chunks at the border of an array always have the full chunk size, even when the array only covers parts of it.
         * For example, having an array with "shape": [30, 30] and "chunk_shape": [16, 16], the chunk 0,1 would also contain
         * unused values for the indices 0-16, 30-31. When writing such chunks it is recommended to use the current fill
         * value for elements outside the bounds of the array.
         */
        final int[] chunkShape = this.metadata.chunkGrid().configuration().chunkShape();

        final int nDim = dimensions.length;
        final DataType dataType = this.dataType;
        final Object fillValue = this.metadata.fillValue();

        final long[] lower  = new long[nDim];
        final long[] upper  = new long[nDim];

        for (int i = 0; i < nDim; i++) {
            if (area != null) {
                lower[i] = area.getLow(i);
                upper[i] = Math.incrementExact(area.getHigh(i));
            } else {
                lower[i] = 0;
                upper[i] = arrayShape[i];
            }
        }

        if (subsampling == null) {
            subsampling = new long[nDim];
            Arrays.fill(subsampling, 1);
        }

        // Calculate output array length after slicing/subsampling
        int totalElements = 1;
        for (int i=0; i<nDim; i++) {
            totalElements *= (int) Math.ceil((double)(upper[i] - lower[i]) / subsampling[i]);
        }

        // Allocate output array of the right type
        Object out = BytesCodec.allocate1DArray(dataType, totalElements);
        if (totalElements == 0) return out;

        // Fill array (if not null or not 0)
        fillArray(out, fillValue);

        // Helper to store current position in the output array
        int[] outStrides = new int[nDim];
        outStrides[nDim - 1] = 1;
        for (int i = nDim - 2; i >= 0; i--)
            outStrides[i] = outStrides[i + 1] * (int) Math.ceil((double)(upper[i + 1] - lower[i + 1]) / subsampling[i + 1]);

        // Pre-calculate chunk strides
        int[] chunkStrides = new int[nDim];
        chunkStrides[nDim - 1] = 1;
        for (int i = nDim - 2; i >= 0; i--) {
            chunkStrides[i] = chunkStrides[i+1] * chunkShape[i+1];
        }

        // Calculate grid bounds to read
        // We determine the range of chunks to loop over [gridMin, gridMax]
        int[] gridMin = new int[nDim];
        int[] gridMax = new int[nDim];
        for (int i = 0; i < nDim; i++) {
            gridMin[i] = (int) (lower[i] / chunkShape[i]);
            // (upper - 1) ensures that if upper is exactly on a chunk boundary, we don't include the next empty chunk
            gridMax[i] = (int) ((upper[i] - 1) / chunkShape[i]);
        }

        // Generate the list of chunk indices we need to read
        List<int[]> chunksTask = new ArrayList<>();
        int[] currentChunkIdx = Arrays.copyOf(gridMin, nDim);
        boolean done = false;

        // Iterate over grid coordinates to build the task list
        while (!done) {
            chunksTask.add(Arrays.copyOf(currentChunkIdx, nDim));
            for (int k = nDim - 1; k >= 0; k--) {
                if (currentChunkIdx[k] < gridMax[k]) {
                    currentChunkIdx[k]++;
                    break;
                }
                currentChunkIdx[k] = gridMin[k];
                if (k == 0) done = true;
            }
        }

        // Execution in parallel (Read)
        long[] finalSubsampling = subsampling;
        chunksTask.parallelStream().forEach(chunkIdx -> {
            try {
                // 1. Compute geometry
                int[] chunkStart = new int[nDim];
                int[] sliceStart = new int[nDim];
                int[] sliceEnd   = new int[nDim];

                for (int d = 0; d < nDim; d++) {
                    chunkStart[d] = chunkIdx[d] * chunkShape[d];
                    sliceStart[d] = (int) Math.max(chunkStart[d], lower[d]);
                    int chunkLimit = Math.min(chunkStart[d] + chunkShape[d], arrayShape[d]);
                    sliceEnd[d]   = (int) Math.min(chunkLimit, upper[d]);
                }

                // 2. Read & Decode
                // Get chunk path
                Path chunkPath = this.metadata.getChunkPath(chunkIdx);
                Object data = readChunkBytes(chunkPath);

                if (data != null) {
                    // List of representation types for the codecs used in this Zarr array.
                    List<ZarrRepresentationType> types = this.metadata.representationTypes();

                    // 3. Decode the chunk data (using the codecs in reverse order)
                    for (int i = metadata.codecs().size() - 1; i >= 0; i--) {
                        AbstractZarrCodec codec = metadata.codecs().get(i);
                        data = codec.decode(data, types.get(i));
                    }

                    // 4. Copy to the output array (thread-safe because each thread writes to distinct regions, there is no overlap)
                    copyChunkRegionToOutput(
                            data, 0, 0, 0,
                            chunkStart, chunkShape, chunkStrides,
                            sliceStart, sliceEnd,
                            lower, finalSubsampling,
                            out, outStrides
                    );
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading chunk " + Arrays.toString(chunkIdx), e);
            }
        });

        return out;
    }

    /**
     * Recursive method to copy a region from a chunk to the output array, considering subsampling.
     * Uses System.arraycopy for the contiguous last dimension when possible, if subsampling is 1 (=> no subsampling).
     *
     * @param chunkData      The source 1D flattened array
     * @param srcBaseOffset  Current offset in chunkData
     * @param dstBaseOffset  Current offset in out
     * @param dim            Current dimension index being processed
     * @param chunkStart     Global coordinates where this chunk starts
     * @param chunkShape     Shape of the chunk
     * @param chunkStrides   Strides for the chunk array
     * @param sliceStart     Global start coordinate to copy (intersected)
     * @param sliceEnd       Global end coordinate to copy (exclusive)
     * @param globalLower    Global selection start (needed for out offset calc)
     * @param subsampling    Subsampling factors
     * @param out            The destination 1D flattened array
     * @param outStrides     Strides for the output array
     */
    private void copyChunkRegionToOutput(Object chunkData, int srcBaseOffset, int dstBaseOffset, int dim,
            int[] chunkStart, int[] chunkShape, int[] chunkStrides, int[] sliceStart, int[] sliceEnd,
            long[] globalLower, long[] subsampling, Object out, int[] outStrides
    ) {
        int nDim = chunkShape.length;

        // 1. Calculate local start/end indices relative to the chunk
        // - Global coordinate: sliceStart[dim]
        // - Chunk Origin:      chunkStart[dim]
        // - Local Index:       sliceStart[dim] - chunkStart[dim]
        int localStart = sliceStart[dim] - chunkStart[dim];
        int count = sliceEnd[dim] - sliceStart[dim];

        // 2. Check if we are at the last dimension (contiguous block)
        // If last dimension, we can copy directly the contiguous block
        if (dim == nDim - 1) {

            // "Fast Way" when no subsampling (subsampling == 1)
            // In this case, we can copy the whole contiguous block using System.arraycopy
            if (subsampling[dim] == 1) {
                int srcPos = srcBaseOffset + localStart;

                // Determine destination position
                // The destination is computed based on the global selection
                // relative coordinate in selection: (sliceStart[dim] - globalLower[dim])
                int outRel = (int) (sliceStart[dim] - globalLower[dim]);
                int dstPos = dstBaseOffset + outRel; // outStrides[last] is always 1

                System.arraycopy(chunkData, srcPos, out, dstPos, count);
            }
            // "Slow Way" when subsampling > 1
            // In this case, we must iterate manually over the elements to copy
            else {
                int step = (int) subsampling[dim];
                int srcPos = srcBaseOffset + localStart;

                // Initial dest pos
                int outRel = (int) ((sliceStart[dim] - globalLower[dim]) / step);
                int dstPos = dstBaseOffset + outRel;

                copyWithSubsampling(chunkData, srcPos, out, dstPos, count, step, chunkData.getClass().getComponentType());
            }
        }
        // 3. Recursive Step for Higher Dimensions
        // Not the last dimension, must iterate over this dimension and recurse
        else {
            int step = (int) subsampling[dim];

            // Loop through the rows/planes of this dimension
            // We iterate in global coordinates from sliceStart to sliceEnd
            for (int i = 0; i < count; i += step) {
                int globalCoord = sliceStart[dim] + i;

                // Calculate Local Coordinate for Source
                int localCoord = globalCoord - chunkStart[dim];
                int nextSrcOffset = srcBaseOffset + (localCoord * chunkStrides[dim]);

                // Calculate Coordinate for Destination
                // How many pixels have we skipped in the global selection?
                int outCoord = (int) ((globalCoord - globalLower[dim]) / subsampling[dim]);
                int nextDstOffset = dstBaseOffset + (outCoord * outStrides[dim]);

                // Recurse
                copyChunkRegionToOutput(
                        chunkData, nextSrcOffset, nextDstOffset, dim + 1,
                        chunkStart, chunkShape, chunkStrides,
                        sliceStart, sliceEnd, globalLower, subsampling, out, outStrides
                );
            }
        }
    }

    /**
     * Copies data from source to destination with subsampling.
     *
     * @param src the source array
     * @param srcPos the starting position in the source array
     * @param dst the destination array
     * @param dstPos the starting position in the destination array
     * @param count number of elements to consider from source
     * @param step the subsampling step
     * @param type the component type of the arrays
     */
    private void copyWithSubsampling(Object src, int srcPos, Object dst, int dstPos, int count, int step, Class<?> type) {
        // Ideally, keep a reference to DataType enum to avoid reflection,
        // but for brevity using 'instanceof' or checking type:

        if (src instanceof float[]) {
            float[] s = (float[]) src; float[] d = (float[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        } else if (src instanceof double[]) {
            double[] s = (double[]) src; double[] d = (double[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        } else if (src instanceof int[]) {
            int[] s = (int[]) src; int[] d = (int[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        } else if (src instanceof short[]) {
            short[] s = (short[]) src; short[] d = (short[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        } else if (src instanceof long[]) {
            long[] s = (long[]) src; long[] d = (long[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        } else if (src instanceof byte[]) {
            byte[] s = (byte[]) src; byte[] d = (byte[]) dst;
            for (int k = 0; k < count; k+=step) d[dstPos++] = s[srcPos + k];
        }
    }

    /**
     * Reads the bytes of the given chunk.
     * If the chunk does not exist, then this method returns {@code null}.
     *
     * @param  chunkPath  path to the chunk file.
     * @return the bytes of the chunk, or {@code null} if the chunk does not exist.
     * @throws IOException if an I/O error occurred while reading the chunk.
     */
    private byte[] readChunkBytes(Path chunkPath) throws IOException {
        if (Files.exists(chunkPath))
            return Files.readAllBytes(chunkPath);
        return null;
    }

    /**
     * Fills the given array with the given value. Only if the value is non-null and non-zero.
     * @param array the array to fill
     * @param value the value to fill the array with
     */
    private void fillArray(Object array, Object value) {
        if (value == null) return;
        boolean isNaN = false;
        if (value instanceof String) {
            String str = (String) value;
            if (str.isEmpty() || str.equals("0") ||
                    str.equalsIgnoreCase("Infinity") ||
                    str.equalsIgnoreCase("-Infinity")) {
                return;
            }
            isNaN = str.equalsIgnoreCase("NaN");
        }
        // Handle Float Array
        if (array instanceof float[]) {
            float v;
            if (value instanceof Number) {
                v = ((Number) value).floatValue();
            } else {
                if (isNaN) {
                    Arrays.fill((float[]) array, Float.NaN);
                }
                return;
            }
            if (v != 0.0f) Arrays.fill((float[]) array, v);
        }
        // Handle Double Array
        else if (array instanceof double[]) {
            double v;
            if (value instanceof Number) {
                v = ((Number) value).doubleValue();
            } else {
                if (isNaN) {
                    Arrays.fill((double[]) array, Double.NaN);
                }
                return;
            }
            if (v != 0.0d) Arrays.fill((double[]) array, v);
        }
        // Handle Integer Array
        else if (array instanceof int[]) {
            if (value instanceof Number) {
                int v = ((Number) value).intValue();
                if (v != 0) Arrays.fill((int[]) array, v);
            }
            // NaN is not defined for integer types
        }
        // Handle Long Array
        else if (array instanceof long[]) {
            if (value instanceof Number) {
                long v = ((Number) value).longValue();
                if (v != 0L) Arrays.fill((long[]) array, v);
            }
            // NaN is not defined for long types
        }
        // Handle Short Array
        else if (array instanceof short[]) {
            if (value instanceof Number) {
                short v = ((Number) value).shortValue();
                if (v != 0) Arrays.fill((short[]) array, v);
            }
            // NaN is not defined for short types
        }
        // Handle Byte Array
        else if (array instanceof byte[]) {
            if (value instanceof Number) {
                byte v = ((Number) value).byteValue();
                if (v != 0) Arrays.fill((byte[]) array, v);
            }
            // NaN is not defined for byte types
        }
    }

    /**
     * Writes the data of this variable.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the data cannot be written.
     */
    public void write() throws IOException, DataStoreException {
        if (this.isCoordinateSystemAxis && (this.values != null)) {
            writeAxis();
        } else if (!this.isCoordinateSystemAxis && (this.values != null)) {
            writeArrayVariable();
        } else if (this.resourceToWrite != null) {
            writeResource();
        } else {
            throw new DataStoreException("No dimension, array or resource to write for variable: " + name);
        }
    }

    /**
     * Writes the resource associated to this variable.
     * This method sets the {@link #dataType} and updates the metadata accordingly.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the resource cannot be written.
     */
    private void writeResource() throws IOException, DataStoreException {
        if (this.resourceToWrite == null) {
            throw new DataStoreException("No resource to write");
        }

        if (this.resourceToWrite instanceof GridCoverageResource) {
            GridCoverageResource gcr = (GridCoverageResource) this.resourceToWrite;

            final int[] shape = this.metadata.shape();
            final int nDim = shape.length;

            // Handle the trivial 1D or 2D cases with a single slice
            if (nDim <= 2) {
                Raster raster = (this.sampleDimensionIndex < 0)
                        ? gcr.read(null).forConvertedValues(true).render(null).getData()
                        : gcr.read(null, this.sampleDimensionIndex).forConvertedValues(true).render(null).getData();

                setDataTypeFromRaster(raster); // Helper, updates this.dataType and metadata
                writeData(raster);
                return;
            }


            // ND: Build all ND-2D slices
            int nOuter = nDim - 2;
            int[] outerShape = Arrays.copyOf(shape, nOuter);
            List<Raster> rasterSlices = new ArrayList<>();
            int[] idx = new int[nOuter];
            do {
                GridExtent extent = buildSliceExtent(idx, shape);
                Raster raster = (this.sampleDimensionIndex < 0)
                        ? gcr.read(new GridGeometry(extent, null, GridOrientation.REFLECTION_Y)).forConvertedValues(true).render(null).getData()
                        : gcr.read(new GridGeometry(extent, null, GridOrientation.REFLECTION_Y), this.sampleDimensionIndex).forConvertedValues(true).render(null).getData();
                setDataTypeFromRaster(raster);
                rasterSlices.add(raster);
            } while (incrementIndex(idx, outerShape));
            writeData(rasterSlices);

        } else {
            throw new DataStoreException("Resource type not supported: " + Classes.getShortClassName(this.resourceToWrite));
        }
    }

    /**
     * Sets the data type of this variable from the given raster.
     * This method updates the {@link #dataType} field and the metadata accordingly.
     * @param raster the raster to get the data type from.
     * @throws DataStoreContentException if the raster data type is not supported.
     */
    private void setDataTypeFromRaster(Raster raster) throws DataStoreContentException {
        int dataTypeSM = raster.getSampleModel().getDataType();
        if (dataTypeSM == DataBuffer.TYPE_FLOAT) {
            this.dataType = DataType.FLOAT;
        } else if (dataTypeSM == DataBuffer.TYPE_DOUBLE) {
            this.dataType = DataType.DOUBLE;
        } else {
            throw new DataStoreContentException("Raster data type not supported (DataBuffer.dataType): " + dataTypeSM + " (only float/double supported)");
        }
        this.metadata.setDataType(dataType);
    }

    /**
     * Builds a grid extent for a 2D slice of the full array.
     * @param outerIdx indices of the outer dimensions.
     * @param fullShape shape of the full array.
     * @return the grid extent for the slice.
     */
    private GridExtent buildSliceExtent(int[] outerIdx, int[] fullShape) {
        int nDim = fullShape.length;
        long[] low = new long[nDim];
        long[] high = new long[nDim];
        for (int i = 0; i < nDim - 2; i++) {
            low[i] = high[i] = outerIdx[i];
        }
        for (int i = nDim - 2; i < nDim; i++) {
            low[i] = 0;
            high[i] = fullShape[i] - 1;
        }
        return new GridExtent(null, low, high, true);
    }

    /**
     * Increments the given index, given the shape of the array.
     * @param idx the index to increment.
     * @param shape the shape of the array.
     * @return {@code true} if the index was incremented, or {@code false} if we reached the end of the array.
     */
    private boolean incrementIndex(int[] idx, int[] shape) {
        for (int i = idx.length - 1; i >= 0; i--) {
            if (++idx[i] < shape[i]) return true;
            idx[i] = 0;
        }
        return false;
    }

    /**
     * Writes the values of this coordinate system axis.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the values cannot be written.
     */
    private void writeAxis() throws IOException, DataStoreException {
        if (this.values == null) {
            throw new DataStoreException("No values provided for axis variable: " + name);
        }
        writeData(this.values);
    }

    /**
     * Writes the values of this array variable.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the values cannot be written.
     */
    private void writeArrayVariable() throws IOException, DataStoreException {
        if (this.values == null) {
            throw new DataStoreException("No values provided for array variable: " + name);
        }
        writeData(this.values);
    }

    /**
     * Writes the given data to the Zarr chunks.
     * @param data can be a {@link Vector} for dimensions axes or a {@link Raster} for data arrays.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the array cannot be written.
     */
    private void writeData(Object data) throws IOException, DataStoreException {
        final int[] arrayShape = this.metadata.shape();
        final int[] chunkShape = this.metadata.chunkGrid().configuration().chunkShape();
        final int[] gridShape  = this.metadata.getChunksGridShape();
        final int nDim         = dimensions.length;

        List<AbstractZarrCodec> codecs = this.metadata.codecs();
        List<ZarrRepresentationType> reprTypes = this.metadata.representationTypes();

        // Iterate over every chunk position
        int[] chunkIdx = new int[nDim];
        Arrays.fill(chunkIdx, 0);

        CHUNK_LOOP:
        while (true) {
            // Chunk origin in element coordinates
            int[] chunkStart = new int[nDim];
            int[] chunkEnd   = new int[nDim];
            for (int d = 0; d < nDim; d++) {
                chunkStart[d] = chunkIdx[d] * chunkShape[d];
                chunkEnd[d]   = Math.min(chunkStart[d] + chunkShape[d], arrayShape[d]);
            }

            int[] chunkShapeRectified = new int[nDim];
            for (int d = 0; d < nDim; d++) {
                chunkShapeRectified[d] = chunkEnd[d] - chunkStart[d];
            }

            // Encode with all codecs in order
            Object encodedChunk = extractChunk(data, arrayShape, chunkStart, chunkEnd, chunkShapeRectified, this.dataType);

            if (!Arrays.equals(chunkShape, chunkShapeRectified)) // edge chunk, needs padding
            {
                encodedChunk = padChunk(
                        encodedChunk,
                        chunkShapeRectified,      // shape of data just extracted
                        chunkShape,               // full chunk shape
                        this.metadata.fillValue(),
                        this.dataType
                );
            }

            for (AbstractZarrCodec codec : codecs) {
                encodedChunk = codec.encode(encodedChunk, reprTypes.get(codecs.indexOf(codec)));
            }

            // Write to disk
            Path chunkPath = this.metadata.getChunkPath(chunkIdx);
            writeChunkBytes(chunkPath, encodedChunk);

            // Increment chunk index
            int dim = nDim - 1;
            while (dim >= 0) {
                chunkIdx[dim]++;
                if (chunkIdx[dim] < gridShape[dim]) break;
                chunkIdx[dim] = 0;
                dim--;
            }
            if (dim < 0) break CHUNK_LOOP;
        }
    }

    /**
     * Pads an extracted array to the full chunk size with the fillValue if smaller at the array edge.
     * @param extractedArr the flattened array of shape actualShape
     * @param actualShape  the dims of the extracted array (e.g., [4,4])
     * @param fullChunkShape the target chunk shape (e.g., [6,6])
     * @param fillValue the value to use for padding
     * @param type the DataType (for primitive array instantiation)
     * @return a flattened primitive array of fullChunkShape filled with extractedArr and padded with fillValue
     */
    private Object padChunk(Object extractedArr, int[] actualShape, int[] fullChunkShape, Object fillValue, DataType type) throws DataStoreContentException {
        int nDim = fullChunkShape.length;
        int totalOut = 1, totalIn = 1;
        for (int d = 0; d < nDim; d++) {
            totalOut *= fullChunkShape[d];
            totalIn  *= actualShape[d];
        }
        Object outArr = BytesCodec.allocate1DArray(type, totalOut);

        // Multi-dimensional copy: for all positions in actualShape, copy from input; rest in outArr get fillValue
        int[] idx = new int[nDim];
        do {
            // For every index in chunk (up to fullChunkShape)
            boolean inbounds = true;
            for (int d = 0; d < nDim; d++) {
                if (idx[d] >= actualShape[d]) { inbounds = false; break; }
            }
            int outFlat = linearIndex(idx, fullChunkShape);
            if (inbounds) {
                int inFlat = linearIndex(idx, actualShape);
                setElement(outArr, outFlat, getElement(extractedArr, inFlat, type), type);
            } else {
                setElement(outArr, outFlat, fillValue, type);
            }
        } while (incrementIndex(idx, fullChunkShape));
        return outArr;
    }

    private Object getElement(Object arr, int index, DataType type) {
        switch (type.number) {
            case Numbers.BYTE:      return ((byte[]) arr)[index];
            case Numbers.SHORT:     return ((short[]) arr)[index];
            case Numbers.CHARACTER: return ((char[]) arr)[index];
            case Numbers.INTEGER:   return ((int[]) arr)[index];
            case Numbers.LONG:      return ((long[]) arr)[index];
            case Numbers.FLOAT:     return ((float[]) arr)[index];
            case Numbers.DOUBLE:    return ((double[]) arr)[index];
            case Numbers.BOOLEAN:   return ((boolean[]) arr)[index];
            default: throw new IllegalArgumentException("Not a supported primitive type: " + type.name());
        }
    }

    private void setElement(Object arr, int index, Object value, DataType type) {
        switch (type.number) {
            case Numbers.BYTE:      ((byte[]) arr)[index]      = ((Number) value).byteValue();   break;
            case Numbers.SHORT:     ((short[]) arr)[index]     = ((Number) value).shortValue();  break;
            case Numbers.CHARACTER: ((char[]) arr)[index]      = (value instanceof Character) ? ((Character) value) : (char) ((Number) value).intValue(); break;
            case Numbers.INTEGER:   ((int[]) arr)[index]       = ((Number) value).intValue();    break;
            case Numbers.LONG:      ((long[]) arr)[index]      = ((Number) value).longValue();   break;
            case Numbers.FLOAT:     ((float[]) arr)[index]     = ((Number) value).floatValue();  break;
            case Numbers.DOUBLE:    ((double[]) arr)[index]    = ((Number) value).doubleValue(); break;
            case Numbers.BOOLEAN:
                ((boolean[]) arr)[index] = (value instanceof Boolean) ? (Boolean)value : ((Number) value).doubleValue() != 0.0;
                break;
            default: throw new IllegalArgumentException("Not a supported primitive type: " + type.name());
        }
    }

    /**
     * Extracts a chunk from the full array.
     * The data can be a {@link Raster} or a {@link Vector}.
     * @param data the data to extract the chunk from.
     * @param arrayShape the shape of the full array.
     * @param chunkStart the start coordinates of the chunk to extract.
     * @param chunkEnd the end coordinates of the chunk to extract.
     * @param chunkShape the shape of the chunk to extract.
     * @param dataType the data type of the array.
     * @return the extracted chunk as a primitive array.
     * @throws DataStoreContentException if the chunk cannot be extracted.
     */
    private Object extractChunk(Object data, int[] arrayShape, int[] chunkStart, int[] chunkEnd, int[] chunkShape, DataType dataType) throws DataStoreContentException {
        int nDim = chunkShape.length;
        Object chunkArr = BytesCodec.allocate1DArray(dataType, Arrays.stream(chunkShape).reduce(1, (a,b) -> a*b));

        // Case 1: If input is a Raster (2D)
        if (data instanceof Raster) {
            return extractChunkFromRaster((Raster) data, chunkStart, chunkEnd, nDim, chunkArr);
        }
        // Case 2: If input is a list of Raster (ND)
        try {
            if (data instanceof List && !((List<?>) data).isEmpty() && ((List<?>) data).get(0) instanceof Raster) {
                return extractChunkFromRasterList((List<Raster>) data, chunkStart, chunkEnd, chunkShape, nDim, chunkArr, dataType);
            }
        } catch (NumberFormatException e) {
            //Do nothing, fall through to next case
            //Happens when data is a Vector of Strings. On this type of data ".get(0)" throws an exception.
        }
        // Case 3: If input is a Vector
        if (data instanceof Vector || data instanceof String[]) {
            return extractChunkFromVector((Vector) data, arrayShape, chunkStart, chunkEnd, chunkShape, chunkArr);
        }

        throw new DataStoreContentException("Data type not supported for chunk extraction: " + Classes.getShortClassName(data));
    }

    /**
     * Extracts a chunk from a Raster object.
     * @param raster the raster to extract the chunk from.
     * @param chunkStart the start coordinates of the chunk to extract.
     * @param chunkEnd the end coordinates of the chunk to extract.
     * @param nDim the number of dimensions of the raster.
     * @param chunkArr the array to store the extracted chunk.
     * @return the extracted chunk as a primitive array.
     * @throws DataStoreContentException if the chunk cannot be extracted.
     */
    private Object extractChunkFromRaster(Raster raster, int[] chunkStart, int[] chunkEnd, int nDim, Object chunkArr) throws DataStoreContentException {
        // Only supports 2D (or strips), so check axes:
        if (nDim < 2)
            throw new DataStoreContentException("Raster requires at least 2 dimensions (x, y)");

        int x = chunkStart[nDim - 2];
        int y = chunkStart[nDim - 1];
        int w = chunkEnd[nDim - 2] - chunkStart[nDim - 2];
        int h = chunkEnd[nDim - 1] - chunkStart[nDim - 1];

        // Flatten all bands/planes as in C-order
        // Ensure the result fits expected primitive array (should be the same as chunkArr)
        // If not, you may need to cast/copy to chunkArr
        return raster.getDataElements(x, y, w, h, chunkArr);
    }

    private Object extractChunkFromRasterList(List<Raster> rasterList, int[] chunkStart, int[] chunkEnd, int[] chunkShape, int nDim, Object chunkArr, DataType dataType) throws DataStoreContentException {
        final int[] shape = this.metadata.shape();
        int nOuter = nDim - 2;
        int[] outerShape = Arrays.copyOf(shape, nOuter);

        int w = chunkEnd[chunkShape.length-2] - chunkStart[chunkShape.length-2];
        int h  = chunkEnd[chunkShape.length-1] - chunkStart[chunkShape.length-1];

        // Loop over outer
        int[] idx = new int[nOuter];
        int outerChunkSize = w*h;
        do {
            int sliceIndex = linearIndex(idx, outerShape);
            Raster sliceRaster = rasterList.get(sliceIndex);

            // Compute starting offset for this ND slice (outer position)
            int baseOffset = linearIndex(idx, Arrays.copyOf(chunkShape, nOuter)) * outerChunkSize;

            Object dtmp = BytesCodec.allocate1DArray(dataType, w);
            for (int y = 0; y < h; y++) {
                sliceRaster.getDataElements(chunkStart[nDim-2], chunkStart[nDim-1] + y, w, 1, dtmp);
                System.arraycopy(dtmp, 0, chunkArr, baseOffset + y * chunkShape[nDim-2], w);
            }
        } while (incrementIndex(idx, outerShape));
        return chunkArr;
    }

    /**
     * Extracts a chunk from a Vector object.
     * @param vector the vector to extract the chunk from.
     * @param arrayShape the shape of the full array.
     * @param chunkStart the start coordinates of the chunk to extract.
     * @param chunkEnd the end coordinates of the chunk to extract.
     * @param chunkShape the shape of the chunk to extract.
     * @param chunkArr the array to store the extracted chunk.
     * @return the extracted chunk as a primitive array.
     * @throws DataStoreContentException if the chunk cannot be extracted.
     */
    private Object extractChunkFromVector(Vector vector, int[] arrayShape, int[] chunkStart, int[] chunkEnd, int[] chunkShape, Object chunkArr) throws DataStoreContentException {
        // coords inside chunk and full array
        int[] coord = Arrays.copyOf(chunkStart, chunkStart.length);

        int[] chunkCoord = new int[chunkStart.length];
        int[] strideFull  = calcStride(arrayShape);
        int[] strideChunk = calcStride(chunkShape);

        while (true) {
            int srcIndex = 0, dstIndex = 0;
            for (int d = 0; d < arrayShape.length; d++) {
                srcIndex += coord[d]       * strideFull[d];
                dstIndex += chunkCoord[d]  * strideChunk[d];
            }

            if (dataType == DataType.STRING) {
                String[] strArr = new String[vector.size()];
                for (int i = 0; i < vector.size(); i++) {
                    strArr[i] = vector.stringValue(i);
                }
                ((String[]) chunkArr)[dstIndex] = strArr[srcIndex];
            } else if (vector != null) {
                // copy element
                switch (dataType.number) {
                    case Numbers.DOUBLE:    ((double[])chunkArr)[dstIndex] = vector.doubleValues()[srcIndex]; break;
                    case Numbers.FLOAT:     ((float[])chunkArr)[dstIndex]  = vector.floatValues()[srcIndex];  break;
                    case Numbers.LONG:      ((long[])chunkArr)[dstIndex]  = vector.longValues()[srcIndex];  break;
                    case Numbers.INTEGER:   ((int[])chunkArr)[dstIndex]   = vector.intValues()[srcIndex];   break;
                    default:
                        throw new DataStoreContentException("Data type not supported for chunk extraction from Vector: " + dataType + ". Only float/double supported (for dimensions axes).");
                }
            }

            // advance coord
            int k = coord.length - 1;
            while (k >= 0) {
                coord[k]++;
                chunkCoord[k]++;
                if (coord[k] < chunkEnd[k]) break;
                coord[k] = chunkStart[k];
                chunkCoord[k] = 0;
                k--;
            }
            if (k < 0) break;
        }

        return chunkArr;
    }

    private int linearIndex(int[] idx, int[] shape) {
        int offset = 0, stride = 1;
        for (int i = idx.length - 1; i >= 0; i--) {
            offset += idx[i]*stride;
            stride *= shape[i];
        }
        return offset;
    }

    /**
     * Calculates the stride for each dimension given the shape of the array.
     * The stride is the number of elements to skip to move to the next element in a given dimension.
     * @param shape the shape of the array.
     * @return the stride for each dimension.
     */
    private int[] calcStride(int[] shape) {
        int[] stride = new int[shape.length];
        stride[shape.length-1] = 1;
        for (int i = shape.length - 2; i >= 0; i--)
            stride[i] = stride[i+1] * shape[i+1];
        return stride;
    }

    /**
     * Writes the given encoded chunk to the given path.
     * @param chunkPath the path to write the chunk to.
     * @param encoded the encoded chunk to write.
     * @throws IOException if an I/O error occurred.
     */
    private void writeChunkBytes(Path chunkPath, Object encoded) throws IOException {
        Files.createDirectories(chunkPath.getParent());
        if (encoded instanceof byte[]) {
            Files.write(chunkPath, (byte[])encoded);
        } else {
            throw new IOException("Encoded chunk is not a byte array.");
        }
    }

    /**
     * Returns the names of all attributes associated to this variable.
     * The returned set is unmodifiable.
     *
     * @return names of all attributes associated to this variable.
     */
    @Override
    public Collection<String> getAttributeNames() {
        return Collections.unmodifiableSet(attributeNames);
    }

    /**
     * Returns the type of the attribute of the given name, or {@code null} if the given attribute is not found.
     * If the attribute contains more than one value, then this method returns a {@code Vector.class} subtype.
     *
     * @param  attributeName  the name of the attribute for which to get the type, preferably in lowercase.
     * @return type of the given attribute, or {@code null} if the attribute does not exist.
     */
    @Override
    public Class<?> getAttributeType(final String attributeName) {
        return Classes.getClass(getAttributeValue(attributeName));
    }

    /**
     * Returns the value of the given attribute, or {@code null} if none.
     * This method should be invoked only for hard-coded names that mix lower-case and upper-case letters.
     *
     * @param  attributeName  name of attribute to search, in the expected case.
     * @param  lowerCase      the all lower-case variant of {@code attributeName}.
     * @return variable attribute value of the given name, or {@code null} if none.
     */
    private Object getAttributeValue(final String attributeName, final String lowerCase) {
        Object value = getAttributeValue(attributeName);
        if (value == null) {
            value = attributes.get(lowerCase);
        }
        return value;
    }

    /**
     * Returns the value of the given attribute, or {@code null} if none.
     * This method does not search the lower-case variant of the given name because the argument given to this method
     * is usually a hard-coded value from {@link CF} or {@link CDM} conventions, which are already in lower-cases.
     *
     * <p>All other {@code getAttributeFoo(…)} methods in this class or parent class ultimately invoke this method.
     * This provides a single point to override if the functionality needs to be extended.</p>
     *
     * @param  attributeName  name of attribute to search, in the expected case.
     * @return variable attribute value of the given name, or {@code null} if none.
     */
    @Override
    protected Object getAttributeValue(final String attributeName) {
        return attributes.get(attributeName);
    }
}
