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
package org.apache.sis.internal.netcdf;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.io.IOException;
import java.awt.image.DataBuffer;
import javax.measure.Unit;
import javax.measure.format.ParserException;
import org.apache.sis.math.Vector;
import org.apache.sis.measure.Units;
import org.apache.sis.internal.system.Modules;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.util.logging.WarningListeners;
import org.apache.sis.util.resources.Errors;


/**
 * A netCDF variable created by {@link Decoder}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Johann Sorel (Geomatys)
 * @version 1.0
 * @since   0.3
 * @module
 */
public abstract class Variable extends NamedElement {
    /**
     * Minimal number of dimension for accepting a variable as a coverage variable.
     */
    public static final int MIN_DIMENSION = 2;

    /**
     * The unit of measurement, parsed from {@link #getUnitsString()} when first needed.
     * We do not try to parse the unit at construction time because this variable may be
     * never requested by the user.
     */
    private Unit<?> unit;

    /**
     * Whether an attempt to parse the unit has already be done. This is used for avoiding
     * to report the same failure many times when {@link #unit} stay null.
     */
    private boolean unitParsed;

    /**
     * Where to report warnings, if any.
     */
    private final WarningListeners<?> listeners;

    /**
     * Creates a new variable.
     *
     * @param listeners where to report warnings.
     */
    protected Variable(final WarningListeners<?> listeners) {
        this.listeners = listeners;
    }

    /**
     * Returns the name of the netCDF file containing this variable, or {@code null} if unknown.
     * This is used for information purpose only.
     *
     * @return name of the netCDF file containing this variable, or {@code null} if unknown.
     */
    public abstract String getFilename();

    /**
     * Returns the name of this variable, or {@code null} if none.
     *
     * @return the name of this variable, or {@code null}.
     */
    @Override
    public abstract String getName();

    /**
     * Returns the description of this variable, or {@code null} if none.
     *
     * @return the description of this variable, or {@code null}.
     */
    public abstract String getDescription();

    /**
     * Returns the unit of measurement as a string, or {@code null} if none.
     *
     * <p>Note: the UCAR library has its own API for handling units (e.g. {@link ucar.nc2.units.SimpleUnit}).
     * However as of November 2018, this API does not allow us to identify the quantity type except for some
     * special cases. We will parse the unit symbol ourselves instead, but we still need the full unit string
     * for parsing also its {@linkplain Axis#direction direction}.</p>
     *
     * @return the unit of measurement, or {@code null}.
     */
    protected abstract String getUnitsString();

    /**
     * Returns the unit of measurement for this variable, or {@code null} if unknown.
     * This method parse the units from {@link #getUnitsString()} when first needed.
     *
     * @return the unit of measurement, or {@code null}.
     */
    public final Unit<?> getUnit() {
        if (!unitParsed) {
            unitParsed = true;                          // Set first for avoiding to report errors many times.
            final String symbols = getUnitsString();
            if (symbols != null) try {
                unit = Units.valueOf(symbols);
            } catch (ParserException e) {
                listeners.warning(Errors.getResources(listeners.getLocale())
                        .getString(Errors.Keys.CanNotAssignUnitToVariable_2, getName(), symbols), e);
            }
        }
        return unit;
    }

    /**
     * Returns the variable data type.
     *
     * @return the variable data type, or {@link DataType#UNKNOWN} if unknown.
     */
    public abstract DataType getDataType();

    /**
     * Returns the name of the variable data type as the name of the primitive type
     * followed by the span of each dimension (in unit of grid cells) between brackets.
     * Example: {@code "SHORT[180][360]"}.
     *
     * @return the name of the variable data type.
     */
    public final String getDataTypeName() {
        final StringBuilder buffer = new StringBuilder(20);
        buffer.append(getDataType().name().toLowerCase());
        final int[] shape = getGridEnvelope();
        for (int i=shape.length; --i>=0;) {
            buffer.append('[').append(Integer.toUnsignedLong(shape[i])).append(']');
        }
        return buffer.toString();
    }

    /**
     * Returns whether this variable can grow. A variable is unlimited if at least one of its dimension is unlimited.
     * In netCDF 3 classic format, only the first dimension can be unlimited.
     *
     * @return whether this variable can grow.
     */
    public abstract boolean isUnlimited();

    /**
     * Returns {@code true} if the given variable can be used for generating an image.
     * This method checks for the following conditions:
     *
     * <ul>
     *   <li>Images require at least {@value #MIN_DIMENSION} dimensions of size equals or greater
     *       than {@code minLength}. They may have more dimensions, in which case a slice will be
     *       taken later.</li>
     *   <li>Exclude axes. Axes are often already excluded by the above condition
     *       because axis are usually 1-dimensional, but some axes are 2-dimensional
     *       (e.g. a localization grid).</li>
     *   <li>Excludes characters, strings and structures, which can not be easily
     *       mapped to an image type. In addition, 2-dimensional character arrays
     *       are often used for annotations and we don't want to confuse them
     *       with images.</li>
     * </ul>
     *
     * @param  minSpan  minimal span (in unit of grid cells) along the dimensions.
     * @return {@code true} if the variable can be considered a coverage.
     */
    public final boolean isCoverage(final int minSpan) {
        int numVectors = 0;                                     // Number of dimension having more than 1 value.
        for (final int length : getGridEnvelope()) {
            if (Integer.toUnsignedLong(length) >= minSpan) {
                numVectors++;
            }
        }
        if (numVectors >= MIN_DIMENSION) {
            final DataType dataType = getDataType();
            if (dataType.rasterDataType != DataBuffer.TYPE_UNDEFINED) {
                return !isCoordinateSystemAxis();
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if this variable seems to be a coordinate system axis instead than the actual data.
     * By netCDF convention, coordinate system axes have the name of one of the dimensions defined in the netCDF header.
     *
     * @return {@code true} if this variable seems to be a coordinate system axis.
     */
    public abstract boolean isCoordinateSystemAxis();

    /**
     * Returns the names of the dimensions of this variable, in the order they are declared in the netCDF file.
     * The dimensions are those of the grid, not the dimensions of the coordinate system.
     *
     * @return the names of all dimension of the grid, in netCDF order (reverse of "natural" order).
     */
    public abstract String[] getGridDimensionNames();

    /**
     * Returns the length (number of cells) of each grid dimension, in the order they are declared in the netCDF file.
     * The length of this array shall be equals to the length of the {@link #getGridDimensionNames()} array.
     *
     * <p>In ISO 19123 terminology, this method returns the upper corner of the grid envelope plus one.
     * The lower corner is always (0, 0, …, 0).</p>
     *
     * @return the number of grid cells for each dimension, in netCDF order (reverse of "natural" order).
     */
    public abstract int[] getGridEnvelope();

    /**
     * Returns the names of all attributes associated to this variable.
     *
     * @return names of all attributes associated to this variable.
     *
     * @todo Remove this method if it still not used.
     */
    public abstract Collection<String> getAttributeNames();

    /**
     * Returns the sequence of values for the given attribute, or an empty array if none.
     * The elements will be of class {@link String} if {@code numeric} is {@code false},
     * or {@link Number} if {@code numeric} is {@code true}.
     *
     * @param  attributeName  the name of the attribute for which to get the values.
     * @param  numeric        {@code true} if the values are expected to be numeric, or {@code false} for strings.
     * @return the sequence of {@link String} or {@link Number} values for the named attribute.
     */
    public abstract Object[] getAttributeValues(String attributeName, boolean numeric);

    /**
     * Returns the value of the given attribute as a string. This is a convenience method
     * for {@link #getAttributeValues(String, boolean)} when a singleton value is expected.
     *
     * @param  attributeName  the name of the attribute for which to get the values.
     * @return the singleton attribute value, or {@code null} if none or ambiguous.
     */
    public abstract String getAttributeString(final String attributeName);

    /**
     * Reads all the data for this variable and returns them as an array of a Java primitive type.
     * Multi-dimensional variables are flattened as a one-dimensional array (wrapped in a vector).
     * Example:
     *
     * {@preformat text
     *   DIMENSIONS:
     *     time: 3
     *     lat : 2
     *     lon : 4
     *
     *   VARIABLES:
     *     temperature (time,lat,lon)
     *
     *   DATA INDICES:
     *     (0,0,0) (0,0,1) (0,0,2) (0,0,3)
     *     (0,1,0) (0,1,1) (0,1,2) (0,1,3)
     *     (1,0,0) (1,0,1) (1,0,2) (1,0,3)
     *     (1,1,0) (1,1,1) (1,1,2) (1,1,3)
     *     (2,0,0) (2,0,1) (2,0,2) (2,0,3)
     *     (2,1,0) (2,1,1) (2,1,2) (2,1,3)
     * }
     *
     * This method may cache the returned vector, at implementation choice.
     *
     * @return the data as an array of a Java primitive type.
     * @throws IOException if an error occurred while reading the data.
     * @throws DataStoreException if a logical error occurred.
     * @throws ArithmeticException if the size of the variable exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     */
    public abstract Vector read() throws IOException, DataStoreException;

    /**
     * Reads a sub-sampled sub-area of the variable.
     * Constraints on the argument values are:
     *
     * <ul>
     *   <li>All arrays length shall be equal to the length of the {@link #getGridEnvelope()} array.</li>
     *   <li>For each index <var>i</var>, value of {@code area[i]} shall be in the range from 0 inclusive
     *       to {@code Integer.toUnsignedLong(getGridEnvelope()[i])} exclusive.</li>
     * </ul>
     *
     * If the variable has more than one dimension, then the data are packed in a one-dimensional vector
     * in the same way than {@link #read()}.
     *
     * @param  areaLower    index of the first value to read along each dimension.
     * @param  areaUpper    index after the last value to read along each dimension.
     * @param  subsampling  sub-sampling along each dimension. 1 means no sub-sampling.
     * @return the data as an array of a Java primitive type.
     * @throws IOException if an error occurred while reading the data.
     * @throws DataStoreException if a logical error occurred.
     * @throws ArithmeticException if the size of the region to read exceeds {@link Integer#MAX_VALUE}, or other overflow occurs.
     */
    public abstract Vector read(int[] areaLower, int[] areaUpper, int[] subsampling) throws IOException, DataStoreException;

    /**
     * Returns the resources to use for warnings or error messages.
     *
     * @return the resources for the locales specified by the given argument.
     */
    protected final Resources resources() {
        return Resources.forLocale(listeners.getLocale());
    }

    /**
     * Reports a warning to the listeners specified at construction time.
     *
     * @param  caller     the caller class to report, preferably a public class.
     * @param  method     the caller method to report, preferable a public method.
     * @param  key        one or {@link Resources.Keys} constants.
     * @param  arguments  values to be formatted in the {@link java.text.MessageFormat} pattern.
     */
    protected final void warning(final Class<?> caller, final String method, final short key, final Object... arguments) {
        final LogRecord record = resources().getLogRecord(Level.WARNING, key, arguments);
        record.setLoggerName(Modules.NETCDF);
        record.setSourceClassName(caller.getCanonicalName());
        record.setSourceMethodName(method);
        listeners.warning(record);
    }

    /**
     * Returns a string representation of this variable for debugging purpose.
     *
     * @return a string representation of this variable.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder(getName()).append(" : ").append(getDataType());
        final int[] shape = getGridEnvelope();
        for (int i=shape.length; --i>=0;) {
            buffer.append('[').append(Integer.toUnsignedLong(shape[i])).append(']');
        }
        if (isUnlimited()) {
            buffer.append(" (unlimited)");
        }
        return buffer.toString();
    }
}
