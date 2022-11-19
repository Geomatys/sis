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
package org.apache.sis.internal.referencing.provider;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Level;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.annotation.XmlTransient;
import javax.measure.Unit;
import javax.measure.quantity.Angle;
import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.parameter.ParameterNotFoundException;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransformFactory;
import org.opengis.referencing.operation.Transformation;
import org.opengis.referencing.operation.NoninvertibleTransformException;
import org.apache.sis.internal.system.DataDirectory;
import org.apache.sis.internal.referencing.Formulas;
import org.apache.sis.internal.referencing.Resources;
import org.apache.sis.internal.util.Strings;
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.parameter.Parameters;
import org.apache.sis.util.collection.Cache;
import org.apache.sis.util.collection.Containers;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.util.resources.Messages;
import org.apache.sis.measure.Units;


/**
 * The provider for <cite>"National Transformation version 2"</cite> (EPSG:9615).
 * This transform requires data that are not bundled by default with Apache SIS.
 *
 * @author  Simon Reynard (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.3
 * @since   0.7
 * @module
 */
@XmlTransient
public final class NTv2 extends AbstractProvider {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -4027618007780159180L;

    /**
     * The operation parameter descriptor for the <cite>"Latitude and longitude difference file"</cite> parameter value.
     * The file extension is typically {@code ".gsb"}. There is no default value.
     *
     * <!-- Generated by ParameterNameTableGenerator -->
     * <table class="sis">
     *   <caption>Parameter names</caption>
     *   <tr><td> EPSG:    </td><td> Latitude and longitude difference file </td></tr>
     * </table>
     * <b>Notes:</b>
     * <ul>
     *   <li>No default value</li>
     * </ul>
     */
    static final ParameterDescriptor<Path> FILE;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    private static final ParameterDescriptorGroup PARAMETERS;
    static {
        final ParameterBuilder builder = builder();
        FILE = builder
                .addIdentifier("8656")
                .addName("Latitude and longitude difference file")
                .create(Path.class, null);
        PARAMETERS = builder
                .addIdentifier("9615")
                .addName("NTv2")
                .createGroup(FILE);
    }

    /**
     * Creates a new provider.
     */
    public NTv2() {
        super(Transformation.class, PARAMETERS,
              EllipsoidalCS.class, 2, false,
              EllipsoidalCS.class, 2, false);
    }

    /**
     * Creates a transform from the specified group of parameter values.
     *
     * @param  factory  the factory to use if this constructor needs to create other math transforms.
     * @param  values   the group of parameter values.
     * @return the created math transform.
     * @throws ParameterNotFoundException if a required parameter was not found.
     * @throws FactoryException if an error occurred while loading the grid.
     */
    @Override
    public MathTransform createMathTransform(final MathTransformFactory factory, final ParameterValueGroup values)
            throws ParameterNotFoundException, FactoryException
    {
        return createMathTransform(NTv2.class, factory, values, 2);
    }

    /**
     * Creates a transform from the specified group of parameter values.
     *
     * @param  provider  the provider which is creating a transform: {@link NTv2} or {@link NTv1}.
     * @param  factory   the factory to use if this constructor needs to create other math transforms.
     * @param  values    the group of parameter values.
     * @param  version   the expected version (1 or 2).
     * @return the created math transform.
     * @throws ParameterNotFoundException if a required parameter was not found.
     * @throws FactoryException if an error occurred while loading the grid.
     */
    static MathTransform createMathTransform(final Class<? extends AbstractProvider> provider,
            final MathTransformFactory factory, final ParameterValueGroup values, final int version)
            throws ParameterNotFoundException, FactoryException
    {
        final Parameters pg = Parameters.castOrWrap(values);
        final DatumShiftGridFile<Angle,Angle> grid = getOrLoad(provider, pg.getMandatoryValue(FILE), version);
        return DatumShiftGridFile.createGeodeticTransformation(provider, factory, grid);
    }

    /**
     * Returns the grid of the given name. This method returns the cached instance if it still exists,
     * or load the grid otherwise.
     *
     * @param  provider  the provider which is creating a transform.
     * @param  file      name of the datum shift grid file to load.
     * @param  version   the expected version (1 or 2).
     */
    @SuppressWarnings("null")
    static DatumShiftGridFile<Angle,Angle> getOrLoad(final Class<? extends AbstractProvider> provider,
            final Path file, final int version) throws FactoryException
    {
        final Path resolved = DataDirectory.DATUM_CHANGES.resolve(file).toAbsolutePath();
        DatumShiftGridFile<?,?> grid = DatumShiftGridFile.CACHE.peek(resolved);
        if (grid == null) {
            final Cache.Handler<DatumShiftGridFile<?,?>> handler = DatumShiftGridFile.CACHE.lock(resolved);
            try {
                grid = handler.peek();
                if (grid == null) {
                    try (ReadableByteChannel in = Files.newByteChannel(resolved)) {
                        DatumShiftGridLoader.startLoading(provider, file);
                        final Loader loader = new Loader(in, file, version);
                        grid = loader.readAllGrids();
                        loader.report(provider);
                    } catch (IOException | NoninvertibleTransformException | RuntimeException e) {
                        throw DatumShiftGridLoader.canNotLoad(provider.getSimpleName(), file, e);
                    }
                    grid = grid.useSharedData();
                }
            } finally {
                handler.putAndUnlock(grid);
            }
        }
        return grid.castTo(Angle.class, Angle.class);
    }




    /**
     * Loaders of NTv2 data. Instances of this class exist only at loading time.
     * More information on that file format can be found with
     * <a href="https://github.com/Esri/ntv2-file-routines">ESRI NTv2 routines</a>.
     *
     * <p>A NTv2 file contains an arbitrary number of sub-files, where each sub-file is a grid.
     * There is at least one grid (the parent), and potentially many sub-grids of higher density.
     * At the beginning is an overview header block of information that is common to all sub-files.
     * Then there is other headers specific to each sub-files.</p>
     *
     * <p>While this loader is primarily targeted at loading NTv2 files, it can also opportunistically
     * read NTv1 files. The two file formats differ by some header records having different names (but
     * same meanings), the possibility to have sub-grids and the presence of accuracy information.</p>
     *
     * @author  Simon Reynard (Geomatys)
     * @author  Martin Desruisseaux (Geomatys)
     * @version 1.1
     * @since   0.7
     * @module
     */
    private static final class Loader extends DatumShiftGridLoader {
        /**
         * Size of a record. This value applies to both the header records and the data records.
         * In the case of header records, this is the size of the key plus the size of the value.
         */
        private static final int RECORD_LENGTH = 16;

        /**
         * Maximum number of characters for a key in a header record.
         * Expected keys are listed in the {@link #TYPES} map.
         */
        private static final int KEY_LENGTH = 8;

        /**
         * Type of data allowed in header records. Each record header identified by a key contains a value
         * of a type hard-coded by the NTv2 specification; the type is not specified in the file itself.
         */
        private enum DataType {STRING, INTEGER, DOUBLE};

        /**
         * Some known keywords that may appear in NTv2 header records, associated the the expected type of values.
         * The type is not encoded in a NTv2 file; it has to be hard-coded in this table. The first 11 entries in
         * this map (ignoring entries marked by "NTv1") are typically found in overview header, and the remaining
         * entries in the sub-grid headers.
         */
        private static final Map<String,DataType> TYPES;
        static {
            final Map<String,DataType> types = new HashMap<>(38);
/* NTv1 */  types.put("HEADER",   DataType.INTEGER);        // Number of header records (replaced by NUM_OREC)
            types.put("NUM_OREC", DataType.INTEGER);        // Number of records in the header - usually 11
            types.put("NUM_SREC", DataType.INTEGER);        // Number of records in the header of sub-grids - usually 11
            types.put("NUM_FILE", DataType.INTEGER);        // Number of sub-grids
/* NTv1 */  types.put("TYPE",     DataType.STRING);         // Grid shift data type (replaced by GS_TYPE)
            types.put("GS_TYPE",  DataType.STRING);         // Units: "SECONDS", "MINUTES" or "DEGREES"
            types.put("VERSION",  DataType.STRING);         // Grid version
/* NTv1 */  types.put("FROM",     DataType.STRING);         // Source CRS (replaced by SYSTEM_F)
/* NTv1 */  types.put("TO",       DataType.STRING);         // Target CRS (replaced by SYSTEM_T)
            types.put("SYSTEM_F", DataType.STRING);         // Source CRS
            types.put("SYSTEM_T", DataType.STRING);         // Target CRS
            types.put("DATUM_F",  DataType.STRING);         // Source datum (some time replace SYSTEM_F)
            types.put("DATUM_T",  DataType.STRING);         // Target datum (some time replace SYSTEM_T)
            types.put("MAJOR_F",  DataType.DOUBLE);         // Semi-major axis of source ellipsoid (in metres)
            types.put("MINOR_F",  DataType.DOUBLE);         // Semi-minor axis of source ellipsoid (in metres)
            types.put("MAJOR_T",  DataType.DOUBLE);         // Semi-major axis of target ellipsoid (in metres)
            types.put("MINOR_T",  DataType.DOUBLE);         // Semi-minor axis of target ellipsoid (in metres)
            types.put("SUB_NAME", DataType.STRING);         // Sub-grid identifier
            types.put("PARENT",   DataType.STRING);         // Parent grid
            types.put("CREATED",  DataType.STRING);         // Creation time
            types.put("UPDATED",  DataType.STRING);         // Update time
            types.put("S_LAT",    DataType.DOUBLE);         // Southmost φ value
            types.put("N_LAT",    DataType.DOUBLE);         // Northmost φ value
            types.put("E_LONG",   DataType.DOUBLE);         // Eastmost λ value - west is positive, east is negative
            types.put("W_LONG",   DataType.DOUBLE);         // Westmost λ value - west is positive, east is negative
/* NTv1 */  types.put("N_GRID",   DataType.DOUBLE);         // Latitude grid interval (replaced by LAT_INC)
/* NTv1 */  types.put("W_GRID",   DataType.DOUBLE);         // Longitude grid interval (replaced by LONG_INC)
            types.put("LAT_INC",  DataType.DOUBLE);         // Increment on φ axis
            types.put("LONG_INC", DataType.DOUBLE);         // Increment on λ axis - positive toward west
            types.put("GS_COUNT", DataType.INTEGER);        // Number of sub-grid records following
            TYPES = types;
            /*
             * NTv1 as two last unnamed records of DataType.DOUBLE: "Semi_Major_Axis_From"
             * and "Semi_Major_Axis_To". Those records are currently ignored.
             */
        }

        /**
         * The headers content, as the union of the overview header and the header in process of being read.
         * Keys are strings like {@code "VERSION"}, {@code "SYSTEM_F"}, {@code "LONG_INC"}, <i>etc.</i>.
         * Values are {@link String}, {@link Integer} or {@link Double}. If some keys are unrecognized,
         * they will be put in this map with the {@code null} value and the {@link #hasUnrecognized} flag
         * will be set to {@code true}.
         */
        private final Map<String,Object> header;

        /**
         * Keys of {@link #header} for entries that were declared in the overview header.
         * This is used after {@link #readGrid(Map, Map)} execution for discarding all
         * entries specific to sub-grids, for avoiding to mix entries from two sub-grids.
         */
        private final String[] overviewKeys;

        /**
         * {@code true} if we are reading a NTv2 file, or {@code false} if we are reading a NTv1 file.
         */
        private final boolean isV2;

        /**
         * {@code true} if the {@code header} map contains at least one key associated to a null value.
         */
        private boolean hasUnrecognized;

        /**
         * Number of grids expected in the file.
         */
        private final int numGrids;

        /**
         * Dates at which the grid has been created or updated, or {@code null} if unknown.
         * Used for information purpose only.
         */
        private String created, updated;

        /**
         * Creates a new reader for the given channel.
         * This constructor parses the header immediately, but does not read any grid.
         * A hint about expected NTv2 version is given, but this constructor may override
         * that hint with information found in the file.
         *
         * @param  channel  where to read data from.
         * @param  file     path to the longitude and latitude difference file.
         *                  Used for parameter declaration and error reporting.
         * @param  version  the expected version (1 or 2).
         * @throws FactoryException if a data record cannot be parsed.
         */
        Loader(final ReadableByteChannel channel, final Path file, int version) throws IOException, FactoryException {
            super(channel, ByteBuffer.allocate(4096), file);
            header = new LinkedHashMap<>();
            ensureBufferContains(RECORD_LENGTH);
            if (isLittleEndian(buffer.getInt(KEY_LENGTH))) {
                buffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            /*
             * Read the overview header. It is normally made of the first 11 records documented in TYPES map:
             * NUM_OREC, NUM_SREC, NUM_FILE, GS_TYPE, VERSION, SYSTEM_F, SYSTEM_T, MAJOR_F, MINOR_F, MAJOR_T,
             * MINOR_T.
             */
            readHeader(version >= 2 ? 11 : 12, "NUM_OREC");
            /*
             * The version number is a string like "NTv2.0". If there is no version number, it is probably NTv1
             * since the "VERSION" record was introduced only in version 2. In such case the `version` parameter
             * should have been 1; in case of doubt we do not modify the provided value.
             */
            final String vs = (String) get("VERSION", false);
            if (vs != null) {
                for (int i=0; i<vs.length(); i++) {
                    final char c = vs.charAt(i);
                    if (c >= '0' && c <= '9') {
                        version = c - '0';
                        break;
                    }
                }
            }
            /*
             * Subgrids are NTv2 features which did not existed in NTv1. If we expect a NTv2 file,
             * the record is mandatory. If we expect a NTv1 file, the record should not be present
             * but we nevertheless check in case we have been misleaded by a missing "VERSION" record.
             */
            final Integer n = (Integer) get("NUM_FILE", (vs != null) && version >= 2);
            isV2 = (n != null);
            if (isV2) {
                numGrids = n;
                if (numGrids < 1) {
                    throw new FactoryException(Errors.format(Errors.Keys.UnexpectedValueInElement_2, "NUM_FILE", n));
                }
            } else {
                numGrids = 1;
            }
            overviewKeys = header.keySet().toArray(new String[header.size()]);
        }

        /**
         * Returns {@code true} if the given value seems to be stored in little endian order.
         * The strategy is to read an integer that we expect to be small (the HEADER or NUM_OREC
         * value which should be 12 or 11) and to check which order gives the smallest value.
         */
        private static boolean isLittleEndian(final int n) {
            return Integer.compareUnsigned(n, Integer.reverseBytes(n)) > 0;
        }

        /**
         * Reads a string at the current buffer position, assuming ASCII encoding.
         * After this method call, the buffer position will be the first byte after
         * the string. The buffer content is unmodified.
         *
         * @param  length  number of bytes to read.
         */
        private String readString(int length) {
            final byte[] array = buffer.array();
            final int position = buffer.position();
            buffer.position(position + length);     // Update before we modify `length`.
            while (length > position && array[position + length - 1] <= ' ') length--;
            return new String(array, position, length, StandardCharsets.US_ASCII).trim();
        }

        /**
         * Reads all records found in the header, starting from the current buffer position.
         * The header may be the overview header (in which case we expect a number of records
         * given by {@code HEADER} or {@code NUM_OREC} value) or a sub-grid header (in which
         * case we expect {@code NUM_SREC} records).
         *
         * <p>The {@code numRecords} given in argument is a default value.
         * It will be updated as soon as the {@code numKey} record is found.</p>
         *
         * @param  numRecords  default number of expected records (usually 11).
         * @param  numkey      key of the record giving the number of records: {@code "NUM_OREC"} or {@code "NUM_SREC"}.
         */
        private void readHeader(int numRecords, final String numkey) throws IOException, FactoryException {
            for (int i=0; i < numRecords; i++) {
                ensureBufferContains(RECORD_LENGTH);
                final String key = readString(KEY_LENGTH).toUpperCase(Locale.US).replace(' ', '_');
                final DataType type = TYPES.get(key);
                final Comparable<?> value;
                if (type == null) {
                    value = null;
                    hasUnrecognized = true;
                } else switch (type) {                              // TODO: check if we can simplify in JDK14.
                    default: throw new AssertionError(type);
                    case STRING: value = readString(RECORD_LENGTH - KEY_LENGTH); break;
                    case DOUBLE: value = buffer.getDouble(); break;
                    case INTEGER: {
                        final int n = buffer.getInt();
                        buffer.position(buffer.position() + Integer.BYTES);
                        if (key.equals(numkey) || key.equals("HEADER")) {
                            /*
                             * HEADER (NTv1), NUM_OREC (NTv2) or NUM_SREC specify the number of records expected
                             * in the header, which may the the header that we are reading right now. If value
                             * applies to the reader we are reading, we need to update `numRecords` on the fly.
                             */
                            numRecords = n;
                        }
                        value = n;
                        break;
                    }
                }
                final Object old = header.put(key, value);
                if (old != null && !old.equals(value)) {
                    throw new FactoryException(Errors.format(Errors.Keys.KeyCollision_1, key));
                }
            }
            if (created == null) created = Strings.trimOrNull((String) get("CREATED", false));
            if (updated == null) updated = Strings.trimOrNull((String) get("UPDATED", false));
        }

        /**
         * Reads all grids and returns the root grid. After reading all grids, this method rearrange
         * them in a child-parent relationship. The result is a tree with a single root containing
         * sub-grids (if any) as children.
         */
        final DatumShiftGridFile<Angle,Angle> readAllGrids() throws IOException, FactoryException, NoninvertibleTransformException {
            final Map<String,      DatumShiftGridFile<Angle,Angle>>  grids    = new HashMap<>(Containers.hashMapCapacity(numGrids));
            final Map<String, List<DatumShiftGridFile<Angle,Angle>>> children = new LinkedHashMap<>();   // Should have few entries.
            while (grids.size() < numGrids) {
                readGrid(grids, children);
            }
            /*
             * Assign the sub-grids to their parent only after we finished to read all grids.
             * Doing this work last is more robust to cases where grids are in random order.
             *
             * Notes: if the parent-child graph contains cycles (deeper than a child declaring itself as its parent),
             *        the grids in cycles will be lost. This is because we need a grid without parent for getting the
             *        graph added in the roots list. There is currently no mechanism for detecting those problems.
             */
            final List<DatumShiftGridFile<Angle,Angle>> roots = new ArrayList<>();
            for (final Map.Entry<String, List<DatumShiftGridFile<Angle,Angle>>> entry : children.entrySet()) {
                final DatumShiftGridFile<Angle,Angle> parent = grids.get(entry.getKey());
                final List<DatumShiftGridFile<Angle,Angle>> subgrids = entry.getValue();
                if (parent != null) {
                    /*
                     * Verify that the children does not declare themselves as their parent.
                     * It may happen if SUB_GRID and PARENT have the same value, typically a
                     * null or empty value if those records were actually unspecified.
                     */
                    for (int i=subgrids.size(); --i >= 0;) {
                        if (subgrids.get(i) == parent) {      // Want identity check, no need for equals(Object).
                            subgrids.remove(i);
                            roots.add(parent);
                            break;
                        }
                    }
                    if (!subgrids.isEmpty()) {
                        parent.setSubGrids(subgrids);
                    }
                } else {
                    roots.addAll(subgrids);
                }
            }
            switch (roots.size()) {
                case 0:  throw new FactoryException(Errors.format(Errors.Keys.CanNotRead_1, file));
                case 1:  return roots.get(0);
                default: return DatumShiftGridGroup.create(file, roots);
            }
        }

        /**
         * Reads the next grid, starting at the current position. A NTv2 file can have many grids.
         * This can be used for grids having different resolutions depending on the geographic area.
         * The first grid can cover a large area with a coarse resolution, and next grids cover smaller
         * areas overlapping the first grid but with finer resolution.
         *
         * <p>NTv2 grids contain also information about shifts accuracy. This is not yet handled by SIS,
         * except for determining an approximate grid cell resolution.</p>
         *
         * @param  addTo     the map where to add the grid with the grid name as the key.
         * @param  children  the map where to add children with the parent name as the key.
         */
        private void readGrid(final Map<String, DatumShiftGridFile<Angle,Angle>> addTo,
                final Map<String, List<DatumShiftGridFile<Angle,Angle>>> children)
                throws IOException, FactoryException, NoninvertibleTransformException
        {
            if (isV2) {
                readHeader((Integer) get("NUM_SREC", null, null), "NUM_SREC");
            }
            /*
             * Extract the geographic bounding box and cell size. While different units are allowed,
             * in practice we usually have seconds of angle. This unit has the advantage of allowing
             * all floating-point values to be integers.
             *
             * Note that the longitude values in NTv2 files are positive WEST.
             */
            final Unit<Angle> unit;
            final double precision;
            final String type = (String) get("GS_TYPE", "TYPE", null);
            if (type.equalsIgnoreCase("SECONDS")) {                 // Most common value
                unit = Units.ARC_SECOND;
                precision = SECOND_PRECISION;                       // Used only as a hint; will not hurt if wrong.
            } else if (type.equalsIgnoreCase("MINUTES")) {
                unit = Units.ARC_MINUTE;
                precision = SECOND_PRECISION / 60;                  // Used only as a hint; will not hurt if wrong.
            } else if (type.equalsIgnoreCase("DEGREES")) {
                unit = Units.DEGREE;
                precision = SECOND_PRECISION / DEGREES_TO_SECONDS;  // Used only as a hint; will not hurt if wrong.
            } else {
                throw new FactoryException(Errors.format(Errors.Keys.UnexpectedValueInElement_2, "GS_TYPE", type));
            }
            final double  ymin     = (Double)  get("S_LAT",    null,     null);
            final double  ymax     = (Double)  get("N_LAT",    null,     null);
            final double  xmin     = (Double)  get("E_LONG",   null,     null);   // Sign reversed compared to usual convention.
            final double  xmax     = (Double)  get("W_LONG",   null,     null);   // Idem.
            final double  dy       = (Double)  get("LAT_INC",  "N_GRID", null);
            final double  dx       = (Double)  get("LONG_INC", "W_GRID", null);   // Positive toward west.
            final Integer declared = (Integer) get("GS_COUNT", false);
            final int     width    = Math.toIntExact(Math.round((xmax - xmin) / dx + 1));
            final int     height   = Math.toIntExact(Math.round((ymax - ymin) / dy + 1));
            final int     count    = Math.multiplyExact(width, height);
            if (declared != null && count != declared) {
                throw new FactoryException(Errors.format(Errors.Keys.UnexpectedValueInElement_2, "GS_COUNT", declared));
            }
            /*
             * Construct the grid. The sign of longitude translations will need to be reversed in order to have
             * longitudes increasing toward East. We set isCellValueRatio = true (by the arguments given to the
             * DatumShiftGridFile constructor) because this is required by InterpolatedTransform implementation.
             * This setting implies that we divide translation values by dx or dy at reading time. Note that this
             * free us from reversing the sign of longitude translations in the code below; instead, this reversal
             * will be handled by grid.coordinateToGrid MathTransform and its inverse.
             */
            final double size = Math.max(dx, dy);
            final DatumShiftGridFile<Angle,Angle> grid;
            if (isV2) {
                final DatumShiftGridFile.Float<Angle,Angle> data;
                data = new DatumShiftGridFile.Float<>(2, unit, unit, true,
                        -xmin, ymin, -dx, dy, width, height, PARAMETERS, file);
                @SuppressWarnings("MismatchedReadAndWriteOfArray") final float[] tx = data.offsets[0];
                @SuppressWarnings("MismatchedReadAndWriteOfArray") final float[] ty = data.offsets[1];
                data.accuracy = Double.NaN;
                for (int i=0; i<count; i++) {
                    ensureBufferContains(4 * Float.BYTES);
                    ty[i] = (float) (buffer.getFloat() / dy);   // Division by dx and dy because isCellValueRatio = true.
                    tx[i] = (float) (buffer.getFloat() / dx);
                    final double accuracy = Math.min(buffer.getFloat() / dy, buffer.getFloat() / dx);
                    if (accuracy > 0 && !(accuracy >= data.accuracy)) {     // Use '!' for replacing the initial NaN.
                        data.accuracy = accuracy;                           // Smallest non-zero accuracy.
                    }
                }
                grid = DatumShiftGridCompressed.compress(data, null, precision / size);
            } else {
                /*
                 * NTv1: same as NTv2 but using double precision and without accuracy information.
                 */
                final DatumShiftGridFile.Double<Angle,Angle> data;
                grid = data = new DatumShiftGridFile.Double<>(2, unit, unit, true,
                        -xmin, ymin, -dx, dy, width, height, PARAMETERS, file);
                @SuppressWarnings("MismatchedReadAndWriteOfArray") final double[] tx = data.offsets[0];
                @SuppressWarnings("MismatchedReadAndWriteOfArray") final double[] ty = data.offsets[1];
                for (int i=0; i<count; i++) {
                    ensureBufferContains(2 * Double.BYTES);
                    ty[i] = buffer.getDouble() / dy;
                    tx[i] = buffer.getDouble() / dx;
                }
            }
            /*
             * We need an estimation of translation accuracy, in order to decide when to stop iterations
             * during inverse transformations. If we did not found that information in the file, compute
             * an arbitrary default accuracy.
             */
            if (!(grid.accuracy > 0)) {                 // Use ! for catching NaN values (paranoiac check).
                grid.accuracy = Units.DEGREE.getConverterTo(unit).convert(Formulas.ANGULAR_TOLERANCE) / size;
            }
            /*
             * Add the grid to two collection. The first collection associates this grid to its name, and the
             * second collection associates the grid to its parent. We do not try to resolve the child-parent
             * relationship here; we will do that after all sub-grids have been read.
             */
            final String name = (String) get("SUB_NAME", numGrids > 1);
            if (addTo.put(name, grid) != null) {
                throw new FactoryException(Errors.format(Errors.Keys.DuplicatedIdentifier_1, name));
            }
            children.computeIfAbsent((String) get("PARENT", numGrids > 1), (k) -> new ArrayList<>()).add(grid);
            /*
             * End of grid parsing. Remove all header entries that are specific to this sub-grid.
             * After this operation, `header` will contain only overview records.
             */
            header.keySet().retainAll(Arrays.asList(overviewKeys));
        }

        /**
         * Gets the value for the given key. If the value is absent, then this method throws an exception
         * if {@code mandatory} is {@code true} or returns {@code null} otherwise.
         *
         * @param  key        key of the value to search.
         * @param  mandatory  whether to throw an exception if the value is not found.
         * @return value associated to the given key, or {@code null} if none and not mandatory.
         */
        private Object get(final String key, final boolean mandatory) throws FactoryException {
            final Object value = header.get(key);
            if (value != null || !mandatory) {
                return value;
            }
            throw new FactoryException(Errors.format(Errors.Keys.PropertyNotFound_2, file, key));
        }

        /**
         * Returns the value for the given key, or thrown an exception if the value is not found.
         * Before to fail if the key is not found, this method searches for a value associated to
         * an alternative name. That alternative should be the name used in legacy NTv1.
         *
         * @param  key  key of the value to search.
         * @param  alt  alternative key name, or name used in NTv1, or {@code null} if none.
         * @param  kv1  name used in NTv1, or {@code null} if none.
         * @return value associated to the given key (never {@code null}).
         */
        private Object get(final String key, final String alt, final String kv1) throws FactoryException {
            Object value = header.get(key);
            if (value == null) {
                value = header.get(alt);
                if (value == null) {
                    value = header.get(kv1);
                    if (value == null) {
                        throw new FactoryException(Errors.format(Errors.Keys.PropertyNotFound_2, file, key));
                    }
                }
            }
            return value;
        }

        /**
         * If we had any warnings during the loading process, report them now.
         *
         * @param  caller  the provider which created this loader.
         */
        void report(final Class<? extends AbstractProvider> caller) {
            try {
                final String source = (String) get("SYSTEM_F", "DATUM_F", "FROM");
                final String target = (String) get("SYSTEM_T", "DATUM_T", "TO");
                log(caller, Resources.forLocale(null).getLogRecord(Level.FINE,
                            Resources.Keys.UsingDatumShiftGrid_4, source, target,
                            (created != null) ? created : "?",
                            (updated != null) ? updated : "?"));
            } catch (FactoryException e) {
                recoverableException(caller, e);
                // Ignore since above code is only for information purpose.
            }
            if (hasUnrecognized) {
                final StringBuilder keywords = new StringBuilder();
                for (final Map.Entry<String,Object> entry : header.entrySet()) {
                    if (entry.getValue() == null) {
                        if (keywords.length() != 0) {
                            keywords.append(", ");
                        }
                        keywords.append(entry.getKey());
                    }
                }
                log(caller, Messages.getResources(null).getLogRecord(Level.WARNING,
                        Messages.Keys.UnknownKeywordInRecord_2, file, keywords.toString()));
            }
        }
    }
}
