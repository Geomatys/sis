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
package org.apache.sis.internal.netcdf.ucar;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import javax.measure.Unit;
import ucar.ma2.Array;
import ucar.ma2.Section;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.VariableIF;
import ucar.nc2.dataset.Enhancements;
import ucar.nc2.dataset.VariableEnhanced;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.units.SimpleUnit;
import ucar.nc2.units.DateUnit;
import org.opengis.referencing.operation.Matrix;
import org.apache.sis.math.Vector;
import org.apache.sis.internal.netcdf.DataType;
import org.apache.sis.internal.netcdf.Decoder;
import org.apache.sis.internal.netcdf.Grid;
import org.apache.sis.internal.netcdf.Variable;
import org.apache.sis.internal.util.UnmodifiableArrayList;
import org.apache.sis.util.logging.WarningListeners;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.measure.Units;
import ucar.nc2.dataset.CoordinateSystem;


/**
 * A {@link Variable} backed by the UCAR netCDF library.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Johann Sorel (Geomatys)
 * @version 1.0
 * @since   0.3
 * @module
 */
final class VariableWrapper extends Variable {
    /**
     * The netCDF variable. This is typically an instance of {@link VariableEnhanced}.
     */
    final VariableIF variable;

    /**
     * The variable without enhancements. May be the same instance than {@link #variable}
     * if that variable was not enhanced. This field is preferred to {@code variable} for
     * fetching attribute values because the {@code "scale_factor"} and {@code "add_offset"}
     * attributes are hidden by {@link VariableEnhanced}. In order to allow metadata reader
     * to find them, we query attributes in the original variable instead.
     */
    private final VariableIF raw;

    /**
     * Creates a new variable wrapping the given netCDF interface.
     */
    VariableWrapper(final WarningListeners<?> listeners, VariableIF v) {
        super(listeners);
        variable = v;
        if (v instanceof VariableEnhanced) {
            v = ((VariableEnhanced) v).getOriginalVariable();
            if (v == null) {
                v = variable;
            }
        }
        raw = v;
    }

    /**
     * Returns the name of the netCDF file containing this variable, or {@code null} if unknown.
     */
    @Override
    public String getFilename() {
        if (variable instanceof ucar.nc2.Variable) {
            String name = ((ucar.nc2.Variable) variable).getDatasetLocation();
            if (name != null) {
                return name.substring(Math.max(name.lastIndexOf('/'), name.lastIndexOf(File.separatorChar)) + 1);
            }
        }
        return null;
    }

    /**
     * Returns the name of this variable, or {@code null} if none.
     */
    @Override
    public String getName() {
        return variable.getShortName();
    }

    /**
     * Returns the description of this variable, or {@code null} if none.
     */
    @Override
    public String getDescription() {
        return variable.getDescription();
    }

    /**
     * Returns the unit of measurement as a string, or an empty strong if none.
     * Note that the UCAR library represents missing unit by an empty string,
     * which is ambiguous with dimensionless unit.
     */
    @Override
    protected String getUnitsString() {
        return variable.getUnitsString();
    }

    /**
     * Parses the given unit symbol and set the {@link #epoch} if the parsed unit is a temporal unit.
     * This method is called by {@link #getUnit()}. This implementation delegates the work to the UCAR
     * library and converts the result to {@link Unit} and {@link java.time.Instant} objects.
     */
    @Override
    protected Unit<?> parseUnit(final String symbols) throws Exception {
        if (TIME_UNIT_PATTERN.matcher(symbols).matches()) {
            /*
             * UCAR library has two methods for getting epoch: getDate() and getDateOrigin().
             * The former adds to the origin the number that may appear before the unit, for example
             * "2 hours since 1970-01-01 00:00:00". If there is no such number, then the two methods
             * are equivalent. It is not clear that adding such number is the right thing to do.
             */
            final DateUnit temporal = new DateUnit(symbols);
            epoch = temporal.getDateOrigin().toInstant();
            return Units.SECOND.multiply(temporal.getTimeUnit().getValueInSeconds());
        } else {
            /*
             * For all other units, we get the base unit (meter, radian, Kelvin, etc.) and multiply by the scale factor.
             * We also need to take the offset in account for constructing the °C unit as a unit shifted from its Kelvin
             * base. The UCAR library does not provide method giving directly this information, so we infer it indirectly
             * by converting the 0 value.
             */
            final SimpleUnit ucar = SimpleUnit.factoryWithExceptions(symbols);
            if (ucar.isUnknownUnit()) {
                return Units.valueOf(symbols);
            }
            final String baseUnit = ucar.getUnitString();
            Unit<?> unit = Units.valueOf(baseUnit);
            final double scale  = ucar.getValue();
            final double offset = ucar.convertTo(0, SimpleUnit.factoryWithExceptions(baseUnit));
            unit = unit.shift(offset);
            if (!Double.isNaN(scale)) {
                unit = unit.multiply(scale);
            }
            return unit;
        }
    }

    /**
     * Returns the variable data type.
     * This method may return {@code UNKNOWN} if the datatype is unknown.
     */
    @Override
    public DataType getDataType() {
        final DataType type;
        switch (variable.getDataType()) {
            case STRING: return DataType.STRING;
            case CHAR:   return DataType.CHAR;
            case BYTE:   type = DataType.BYTE;   break;
            case SHORT:  type = DataType.SHORT;  break;
            case INT:    type = DataType.INT;    break;
            case LONG:   type = DataType.INT64;  break;
            case FLOAT:  return DataType.FLOAT;
            case DOUBLE: return DataType.DOUBLE;
            default:     return DataType.UNKNOWN;
        }
        return type.unsigned(variable.isUnsigned());
    }

    /**
     * Returns whether this variable can grow. A variable is unlimited if at least one of its dimension is unlimited.
     */
    @Override
    public boolean isUnlimited() {
        return variable.isUnlimited();
    }

    /**
     * Returns {@code true} if this variable seems to be a coordinate system axis.
     */
    @Override
    public boolean isCoordinateSystemAxis() {
        return variable.isCoordinateVariable();
    }

    /**
     * Returns the grid geometry for this variable, or {@code null} if this variable is not a data cube.
     * This method searches for a grid previously computed by {@link DecoderWrapper#getGridGeometries()}.
     * The same grid geometry may be shared by many variables.
     *
     * @see DecoderWrapper#getGridGeometries()
     */
    @Override
    public Grid getGridGeometry(final Decoder decoder) throws IOException, DataStoreException {
        if (variable instanceof Enhancements) {
            final List<CoordinateSystem> cs = ((Enhancements) variable).getCoordinateSystems();
            if (cs != null && !cs.isEmpty()) {
                for (final Grid grid : decoder.getGridGeometries()) {
                    if (cs.contains(((GridWrapper) grid).netcdfCS)) {
                        return grid;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns the names of the dimensions of this variable.
     * The dimensions are those of the grid, not the dimensions of the coordinate system.
     * This information is used for completing ISO 19115 metadata.
     */
    @Override
    public String[] getGridDimensionNames() {
        final List<Dimension> dimensions = variable.getDimensions();
        final String[] names = new String[dimensions.size()];
        for (int i=0; i<names.length; i++) {
            names[i] = dimensions.get(i).getShortName();
        }
        return names;
    }

    /**
     * Returns the length (number of cells) of each grid dimension. In ISO 19123 terminology, this method
     * returns the upper corner of the grid envelope plus one. The lower corner is always (0,0,…,0).
     * This method is used mostly for building string representations of this variable.
     */
    @Override
    public int[] getShape() {
        return variable.getShape();
    }

    /**
     * Returns the names of all attributes associated to this variable.
     *
     * @return names of all attributes associated to this variable.
     */
    @Override
    public Collection<String> getAttributeNames() {
        return toNames(variable.getAttributes());
    }

    /**
     * Returns the sequence of values for the given attribute, or an empty array if none.
     * The elements will be of class {@link String} if {@code numeric} is {@code false},
     * or {@link Number} if {@code numeric} is {@code true}.
     */
    @Override
    public Object[] getAttributeValues(final String attributeName, final boolean numeric) {
        final Attribute attribute = raw.findAttributeIgnoreCase(attributeName);
        if (attribute != null) {
            boolean hasValues = false;
            final Object[] values = new Object[attribute.getLength()];
            for (int i=0; i<values.length; i++) {
                if (numeric) {
                    if ((values[i] = attribute.getNumericValue(i)) != null) {
                        hasValues = true;
                    }
                } else {
                    Object value = attribute.getValue(i);
                    if (value != null) {
                        String text = value.toString().trim();
                        if (!text.isEmpty()) {
                            values[i] = text;
                            hasValues = true;
                        }
                    }
                }
            }
            if (hasValues) {
                return values;
            }
        }
        return new Object[0];
    }

    /**
     * Returns the names of all attributes in the given list.
     */
    static List<String> toNames(final List<Attribute> attributes) {
        final String[] names = new String[attributes.size()];
        for (int i=0; i<names.length; i++) {
            names[i] = attributes.get(i).getShortName();
        }
        return UnmodifiableArrayList.wrap(names);
    }

    /**
     * Whether {@link #read()} invokes {@link Vector#compress(double)} on the returned vector.
     *
     * @return {@code false}.
     */
    @Override
    protected boolean readTriesToCompress() {
        return false;
    }

    /**
     * Reads all the data for this variable and returns them as an array of a Java primitive type.
     * Multi-dimensional variables are flattened as a one-dimensional array (wrapped in a vector).
     * This method may cache the returned vector, at UCAR library choice.
     */
    @Override
    public Vector read() throws IOException {
        final Array array = variable.read();                // May be cached by the UCAR library.
        return Vector.create(array.get1DJavaArray(array.getElementType()), variable.isUnsigned());
    }

    /**
     * Reads a sub-sampled sub-area of the variable.
     *
     * @param  areaLower    index of the first value to read along each dimension.
     * @param  areaUpper    index after the last value to read along each dimension.
     * @param  subsampling  sub-sampling along each dimension. 1 means no sub-sampling.
     * @return the data as an array of a Java primitive type.
     */
    @Override
    public Vector read(final int[] areaLower, final int[] areaUpper, final int[] subsampling)
            throws IOException, DataStoreException
    {
        final int[] size = new int[areaUpper.length];
        for (int i=0; i<size.length; i++) {
            size[i] = areaUpper[i] - areaLower[i];
        }
        final Array array;
        try {
            array = variable.read(new Section(areaLower, size, subsampling));
        } catch (InvalidRangeException e) {
            throw new DataStoreException(e);
        }
        return createDecimalVector(array.get1DJavaArray(array.getElementType()), variable.isUnsigned());
    }

    /**
     * Sets the scale and offset coefficients in the given "grid to CRS" transform if possible.
     * This method is invoked only for variables that represent a coordinate system axis.
     */
    @Override
    protected boolean trySetTransform(final Matrix gridToCRS, final int srcDim, final int tgtDim, final Vector values)
            throws IOException, DataStoreException
    {
        if (variable instanceof CoordinateAxis1D) {
            final CoordinateAxis1D axis = (CoordinateAxis1D) variable;
            if (axis.isRegular()) {
                gridToCRS.setElement(tgtDim, srcDim, axis.getIncrement());
                gridToCRS.setElement(tgtDim, gridToCRS.getNumCol() - 1, axis.getStart());
                return true;
            }
        }
        return super.trySetTransform(gridToCRS, srcDim, tgtDim, values);
    }

    /**
     * Returns {@code true} if this Apache SIS variable is a wrapper for the given UCAR variable.
     */
    final boolean isWrapperFor(final VariableIF v) {
        return (variable == v) || (raw == v);
    }
}
