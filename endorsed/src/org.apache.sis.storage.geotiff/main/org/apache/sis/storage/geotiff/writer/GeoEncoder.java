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
package org.apache.sis.storage.geotiff.writer;

import java.util.List;
import java.util.EnumMap;
import java.util.logging.Level;
import static javax.imageio.plugins.tiff.GeoTIFFTagSet.TAG_GEO_ASCII_PARAMS;
import static javax.imageio.plugins.tiff.GeoTIFFTagSet.TAG_GEO_DOUBLE_PARAMS;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.IncommensurableException;
import javax.measure.quantity.Angle;
import javax.measure.quantity.Length;
import org.opengis.util.FactoryException;
import org.opengis.metadata.Identifier;
import org.opengis.metadata.spatial.CellGeometry;
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeodeticCRS;
import org.opengis.referencing.crs.ProjectedCRS;
import org.opengis.referencing.crs.VerticalCRS;
import org.opengis.referencing.crs.EngineeringCRS;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CartesianCS;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.datum.Ellipsoid;
import org.opengis.referencing.datum.PrimeMeridian;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.datum.VerticalDatum;
import org.opengis.referencing.operation.Conversion;
import org.opengis.referencing.operation.Matrix;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.apache.sis.measure.Units;
import org.apache.sis.util.ArraysExt;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.util.privy.Strings;
import org.apache.sis.util.privy.CollectionsExt;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.IdentifiedObjects;
import org.apache.sis.referencing.datum.PseudoDatum;
import org.apache.sis.referencing.cs.CoordinateSystems;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.referencing.factory.UnavailableFactoryException;
import org.apache.sis.referencing.privy.ReferencingUtilities;
import org.apache.sis.referencing.privy.WKTKeywords;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.PixelInCell;
import org.apache.sis.coverage.grid.IncompleteGridGeometryException;
import org.apache.sis.storage.IncompatibleResourceException;
import org.apache.sis.storage.base.MetadataFetcher;
import org.apache.sis.storage.geotiff.base.UnitKey;
import org.apache.sis.storage.geotiff.base.GeoKeys;
import org.apache.sis.storage.geotiff.base.GeoCodes;
import org.apache.sis.storage.geotiff.base.Resources;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.pending.jdk.JDK15;


/**
 * Helper class for writing GeoKeys.
 * This class decomposes a CRS into entries written by calls to {@code writeShort(…)}, {@code writeDouble(…)}
 * or {@code writeString(…)} methods. The order in which those methods are invoked matter, because the GeoTIFF
 * specification requires that keys are sorted in increasing order. We do not sort them after writing.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class GeoEncoder {
    /**
     * Size of the model transformation matrix, in number of rows and columns.
     * This size is fixed by the GeoTIFF specification.
     */
    private static final int MATRIX_SIZE = 4;

    /**
     * The listeners where to report warnings.
     */
    private final StoreListeners listeners;

    /**
     * Overall configuration of the GeoTIFF file, or {@code null} if none.
     * This is the value to store in {@link GeoKeys#Citation}.
     */
    private String citation;

    /**
     * The coordinate reference system of the grid geometry, or {@code null} if none.
     * This CRS may contain more dimensions than the 3 dimensions allowed by GeoTIFF.
     * Axis order and axis directions may be different than the (east, north, up) directions mandated by GeoTIFF.
     */
    private CoordinateReferenceSystem fullCRS;

    /**
     * Whether the coordinate system has a vertical component.
     */
    private boolean hasVerticalAxis;

    /**
     * The conversion from grid coordinates to full CRS, which determines the model transformation.
     * This conversion may operate on more dimensions than the three dimensions mandated by GeoTIFF.
     * Furthermore the output may need to be reordered for the (east, north, up) axis order mandated by GeoTIFF.
     *
     * @see #modelTransformation()
     */
    private Matrix gridToCRS;

    /**
     * Whether the raster model is "point" or "area".
     * The default is area ({@code false}).
     */
    private boolean isPoint;

    /**
     * Units of measurement found by the analysis of coordinate system axes.
     * Should be filled as soon as possible because it determines also the
     * units of measurement to use for encoding map projection parameters.
     */
    private final EnumMap<UnitKey, Unit<?>> units;

    /**
     * The key directory, including the header.
     * Each entry is a record of {@value GeoCodes#ENTRY_LENGTH} values.
     * The first record is a header of the same length.
     *
     * @see #keyCount
     * @see #keyDirectory()
     */
    private final short[] keyDirectory;

    /**
     * Number of valid elements in {@link #keyDirectory}, not counting the header.
     */
    private int keyCount;

    /**
     * Parameters to encode as IEEE-754 floating point values.
     *
     * @see #doubleCount
     * @see #doubleParams()
     */
    private final double[] doubleParams;

    /**
     * Number of valid elements in {@link #doubleParams}.
     */
    private int doubleCount;

    /**
     * Parameters to encode as ASCII character strings.
     * Strings are separated by the {@value GeoCodes#STRING_SEPARATOR} character.
     *
     * @see #asciiParams()
     */
    private final StringBuilder asciiParams;

    /**
     * If multiple names are packed in a single citation GeoKey, the citation key of the main object.
     * This is a sub-encoding applied inside {@link #asciiParams} for the citation. Example:
     *
     * <pre>GCS Name=Moon 2000|Datum=D_Moon_2000|Ellipsoid=Moon_2000_IAU_IAG|Primem=Reference_Meridian|AUnits=Decimal_Degree|</pre>
     *
     * Above sub-encoding is applied only if necessary. In such case, this field is the first key to prepend.
     * Currently the only accepted value is: "GCS Name".
     */
    private String citationMainKey;

    /**
     * Index in the {@link #keyDirectory} array where the length (in number of characters) of current citation is stored.
     * The {@code keyDirectory[citationLengthIndex]} value is the number of characters, excluding the trailing separator.
     * The {@code keyDirectory[citationLengthIndex+1]} value is the offset where the citation starts.
     * This information is used for modifying in-place the ASCII entry of a citation for inserting more names.
     */
    private int citationLengthIndex;

    /**
     * Whether to disable attempts to write EPSG codes. This is set to {@code true} on the first attempt to use the
     * EPSG database if it appears to be unavailable. This is used for avoiding many retries which will continue to
     * fail.
     */
    private boolean disableEPSG;

    /**
     * Prepares information for writing GeoTIFF tags for the given grid geometry.
     * Caller shall invoke {@link #write(GridGeometry, MetadataFetcher)} exactly once after construction.
     *
     * @param  listeners  the listeners where to report warnings.
     */
    public GeoEncoder(final StoreListeners listeners) {
        this.listeners = listeners;
        units        = new EnumMap<>(UnitKey.class);
        asciiParams  = new StringBuilder(100);
        doubleParams = new double[GeoCodes.NUM_DOUBLE_GEOKEYS];
        keyDirectory = new short[(GeoCodes.NUM_GEOKEYS + 1) * GeoCodes.ENTRY_LENGTH];
        keyDirectory[0] = 1;            // Directory version.
        keyDirectory[1] = 1;            // Revision major number. We implement GeoTIFF 1.1.
        keyDirectory[2] = 1;            // Revision minor number. We implement GeoTIFF 1.1.
    }

    /**
     * Writes GeoTIFF keys for the given grid geometry.
     * This method should be invoked exactly once.
     *
     * @param  store     the store for which to write GeoTIFF keys.
     * @param  grid      grid geometry of the image to write.
     * @param  metadata  overall configuration information.
     * @throws FactoryException if an error occurred while fetching the EPSG code.
     * @throws ArithmeticException if a short value cannot be stored as an unsigned 16 bits integer.
     * @throws IncommensurableException if a measure uses an unexpected unit of measurement.
     * @throws IncompleteGridGeometryException if the grid geometry is incomplete.
     * @throws IncompatibleResourceException if the grid geometry cannot be encoded.
     */
    public void write(final GridGeometry grid, final MetadataFetcher<?> metadata)
            throws FactoryException, IncommensurableException, IncompatibleResourceException
    {
        citation  = CollectionsExt.first(metadata.transformationDimension);
        isPoint   = CollectionsExt.first(metadata.cellGeometry) == CellGeometry.POINT;
        gridToCRS = MathTransforms.getMatrix(grid.getGridToCRS(isPoint ? PixelInCell.CELL_CENTER : PixelInCell.CELL_CORNER));
        if (gridToCRS == null) {
            String message = resources().getString(Resources.Keys.CanNotEncodeNonLinearModel);
            throw new IncompatibleResourceException(message).addAspect("gridToCRS");
        }
        if (grid.isDefined(GridGeometry.CRS)) {
            fullCRS = grid.getCoordinateReferenceSystem();
            final CoordinateReferenceSystem crs = CRS.getHorizontalComponent(fullCRS);
            if (crs instanceof ProjectedCRS) {
                writeCRS((ProjectedCRS) crs);
                writeCRS(CRS.getVerticalComponent(fullCRS, true));
            } else if (crs instanceof GeodeticCRS) {
                writeCRS((GeodeticCRS) crs, false);
                writeCRS(CRS.getVerticalComponent(fullCRS, true));
            } else if (fullCRS instanceof EngineeringCRS && ReferencingUtilities.getDimension(fullCRS) == 2) {
                writeModelType(GeoCodes.userDefined);
            } else {
                throw unsupportedType(fullCRS);
            }
        } else {
            writeModelType(GeoCodes.undefined);
        }
    }

    /**
     * Writes the first keys (model type, raster type, citation).
     * This method shall be the first write operation, before to write any other keys.
     *
     * @param  type  value of {@link GeoKeys#ModelType}.
     */
    private void writeModelType(final short type) {
        writeShort(GeoKeys.ModelType, type);
        writeShort(GeoKeys.RasterType, isPoint ? GeoCodes.RasterPixelIsPoint : GeoCodes.RasterPixelIsArea);
        if (citation != null) {
            writeString(GeoKeys.Citation, citation);
            citation = null;
        }
    }

    /**
     * Writes the vertical component of the CRS.
     * The horizontal component must have been written before this method is invoked.
     *
     * @param  crs  the CRS to write, or {@code null} if none.
     * @throws FactoryException if an error occurred while fetching an EPSG code.
     * @throws IncompatibleResourceException if a unit of measurement cannot be encoded.
     */
    private void writeCRS(final VerticalCRS crs) throws FactoryException, IncompatibleResourceException {
        if (crs != null) {
            hasVerticalAxis = true;
            if (writeEPSG(GeoKeys.Vertical, crs)) {
                writeName(GeoKeys.VerticalCitation, null, crs);
                addUnits(UnitKey.VERTICAL, crs.getCoordinateSystem());
                final VerticalDatum datum = PseudoDatum.of(crs);
                if (writeEPSG(GeoKeys.VerticalDatum, datum)) {
                    /*
                     * OGC requirement 25.5 said "VerticalCitationGeoKey SHALL be populated."
                     * But how? Using the same multiple-names convention as for geodetic CRS?
                     *
                     * https://github.com/opengeospatial/geotiff/issues/59
                     */
                }
                writeUnit(UnitKey.VERTICAL);
            }
        }
    }

    /**
     * Writes entries for a geographic or geocentric CRS.
     * The CRS type is inferred from the coordinate system type.
     * This method may be invoked for writing the base CRS of a projected CRS.
     *
     * @param  crs        the CRS to write.
     * @param  isBaseCRS  whether to write the base CRS of a projected CRS.
     * @throws FactoryException if an error occurred while fetching an EPSG code.
     * @throws IncommensurableException if a measure uses an unexpected unit of measurement.
     * @throws IncompatibleResourceException if the <abbr>CRS</abbr> has an incompatible property.
     */
    private void writeCRS(final GeodeticCRS crs, final boolean isBaseCRS)
            throws FactoryException, IncommensurableException, IncompatibleResourceException
    {
        final short type;
        final CoordinateSystem cs = crs.getCoordinateSystem();
        addUnits(UnitKey.ANGULAR, cs);
        if (cs instanceof EllipsoidalCS) {
            type = GeoCodes.ModelTypeGeographic;
        } else if (isBaseCRS) {
            String message = resources().getString(Resources.Keys.CanNotEncodeNonGeographicBase);
            throw new IncompatibleResourceException(message).addAspect("crs");
        } else if (cs instanceof CartesianCS) {
            type = GeoCodes.ModelTypeGeocentric;
        } else {
            throw unsupportedType(cs);
        }
        /*
         * Start writing GeoTIFF keys for the geodetic CRS, potentially followed by datum, prime meridian and ellipsoid
         * in that order. The order matter because GeoTIFF specification requires keys to be sorted in increasing order.
         * A difficulty is that units of measurement are between prime meridian and ellipsoid, and the angular unit is
         * needed for projected CRS too.
         */
        writeModelType(isBaseCRS ? GeoCodes.ModelTypeProjected : type);
        if (writeEPSG(GeoKeys.GeodeticCRS, crs)) {
            writeName(GeoKeys.GeodeticCitation, "GCS Name", crs);
            final GeodeticDatum  datum = PseudoDatum.of(crs);
            if (writeEPSG(GeoKeys.GeodeticDatum, datum)) {
                appendName(WKTKeywords.Datum, datum);
                final PrimeMeridian primem = datum.getPrimeMeridian();
                final double longitude;
                if (writeEPSG(GeoKeys.PrimeMeridian, primem)) {
                    appendName(WKTKeywords.PrimeM, datum);
                    longitude = primem.getGreenwichLongitude();
                } else {
                    longitude = 0;                                      // Means "do not write prime meridian".
                }
                final Ellipsoid     ellipsoid  = datum.getEllipsoid();
                final Unit<Length>  axisUnit   = ellipsoid.getAxisUnit();
                final Unit<?>       linearUnit = units.putIfAbsent(UnitKey.LINEAR, axisUnit);
                final UnitConverter toLinear   = axisUnit.getConverterToAny(linearUnit != null ? linearUnit : axisUnit);
                writeUnit(UnitKey.LINEAR);     // Must be after the `units` map have been updated.
                writeUnit(UnitKey.ANGULAR);
                if (writeEPSG(GeoKeys.Ellipsoid, ellipsoid)) {
                    appendName(WKTKeywords.Ellipsoid, ellipsoid);
                    writeDouble(GeoKeys.SemiMajorAxis, toLinear.convert(ellipsoid.getSemiMajorAxis()));
                    if (ellipsoid.isSphere() || !ellipsoid.isIvfDefinitive()) {
                        writeDouble(GeoKeys.SemiMinorAxis, toLinear.convert(ellipsoid.getSemiMinorAxis()));
                    } else {
                        writeDouble(GeoKeys.InvFlattening, ellipsoid.getInverseFlattening());
                    }
                }
                if (longitude != 0) {
                    Unit<Angle> unit = primem.getAngularUnit();
                    UnitConverter c = unit.getConverterToAny(units.getOrDefault(UnitKey.ANGULAR, Units.DEGREE));
                    writeDouble(GeoKeys.PrimeMeridianLongitude, c.convert(longitude));
                }
            }
        } else if (isBaseCRS) {
            writeUnit(UnitKey.ANGULAR);         // Map projection parameters may need this unit.
        }
    }

    /**
     * Writes entries for a projected CRS.
     * If the CRS is user-specified, then this method writes the geodetic CRS first.
     *
     * @return whether this method has been able to write the CRS.
     * @throws FactoryException if an error occurred while fetching an EPSG or GeoTIFF code.
     * @throws IncommensurableException if a measure uses an unexpected unit of measurement.
     * @throws IncompatibleResourceException if the <abbr>CRS</abbr> has an incompatible property.
     */
    private boolean writeCRS(final ProjectedCRS crs)
            throws FactoryException, IncommensurableException, IncompatibleResourceException
    {
        writeCRS(crs.getBaseCRS(), true);
        if (writeEPSG(GeoKeys.ProjectedCRS, crs)) {
            writeName(GeoKeys.ProjectedCitation, null, crs);
            addUnits(UnitKey.PROJECTED, crs.getCoordinateSystem());
            final Conversion projection = crs.getConversionFromBase();
            if (writeEPSG(GeoKeys.Projection, projection)) {
                final var method = projection.getMethod();
                final short projCode = getGeoCode(0, method);
                writeShort(GeoKeys.ProjMethod, projCode);
                writeUnit(UnitKey.PROJECTED);
                switch (projCode) {
                    case GeoCodes.userDefined:  // Should not happen.
                    case GeoCodes.undefined: {
                        missingValue(GeoKeys.ProjMethod);
                        return true;
                    }
                    /*
                     * TODO: GeoTIFF requirement 27.4 said that ProjectedCitationGeoKey shall be provided,
                     * But how? Using the same multiple-names convention ("GCS Name") as for geodetic CRS?
                     *
                     * https://github.com/opengeospatial/geotiff/issues/59
                     */
                }
            }
            for (final GeneralParameterValue p : projection.getParameterValues().values()) {
                RuntimeException cause = null;
                final var descriptor = p.getDescriptor();
                if (p instanceof ParameterValue<?>) {
                    final short key = getGeoCode(1, descriptor);
                    if (key != GeoCodes.undefined && key != GeoCodes.userDefined) {
                        final var pv = (ParameterValue<?>) p;
                        final UnitKey type = UnitKey.ofProjectionParameter(key);
                        if (type == UnitKey.LINEAR) {
                            continue;                   // Skip the "cannot encode" error.
                        }
                        if (type != UnitKey.NULL) try {
                            final Unit<?> unit = units.getOrDefault(type, type.defaultUnit());
                            writeDouble(key, (unit != null) ? pv.doubleValue(unit) : pv.doubleValue());
                            continue;
                        } catch (IllegalArgumentException | IllegalStateException e) {
                            cause = e;
                        }
                    }
                }
                throw cannotEncode(1, name(descriptor), cause);
            }
        }
        return true;
    }

    /**
     * Remembers the units of measurement found in all coordinate system axes.
     * The units are stored in the {@link #units} map.
     *
     * @param  main  the main kind of units expected in the coordinate system.
     * @param  cs    the coordinate system to analyze.
     * @throws IncompatibleResourceException if the unit of measurement cannot be encoded.
     */
    private void addUnits(final UnitKey main, final CoordinateSystem cs) throws IncompatibleResourceException {
        for (int i = cs.getDimension(); --i >= 0;) {
            final Unit<?> unit = cs.getAxis(i).getUnit();
            final UnitKey type = main.validate(unit);
            if (type != null) {
                final Unit<?> previous = units.putIfAbsent(type, unit);
                if (previous != null && !previous.equals(unit)) {
                    String message = errors().getString(Errors.Keys.HeterogynousUnitsIn_1, name(cs));
                    throw new IncompatibleResourceException(message).addAspect("crs");
                }
            } else {
                throw cannotEncode(2, unit.toString(), null).addAspect("unit");
            }
        }
    }

    /**
     * Writes the entries for the specified unit of measurement.
     * This method should be invoked only once per unit key.
     *
     * @param  key  identification of the unit to write.
     * @throws IncompatibleResourceException if the unit of measurement cannot be encoded.
     */
    private void writeUnit(final UnitKey key) throws IncompatibleResourceException {
        final Unit<?> unit = units.get(key);
        if (unit != null) {
            final short epsg = toShortEPSG(Units.getEpsgCode(unit, key.isAxis));
            if (epsg != GeoCodes.userDefined) {
                writeShort(key.codeKey, epsg);
            } else if (key.scaleKey != 0) {
                writeShort(key.codeKey, epsg);
                writeDouble(key.scaleKey, Units.toStandardUnit(unit));
            } else {
                throw cannotEncode(2, unit.toString(), null).addAspect("unit");
            }
        }
    }

    /**
     * Writes the name of the specified object.
     *
     * @param  key     the numeric identifier of the GeoTIFF key.
     * @param  type    type of object for which to write the name, or {@code null} for no multiple-names citation.
     * @param  object  the object for which to write the name.
     */
    private void writeName(final short key, final String type, final IdentifiedObject object) {
        String name = IdentifiedObjects.getName(object, null);
        if (name == null) {
            name = "Unnamed";
        }
        writeString(key, name);
        citationMainKey = type;
        citationLengthIndex = keyCount * GeoCodes.ENTRY_LENGTH + 2;        // Length is the field #2.
    }

    /**
     * Writes the name of the specified object using the "multi-names in single citation" convention.
     * The {@link #writeName(short, String, IdentifiedObject)} method must have been invoked for the
     * main object before this method call.
     *
     * @param  type    type of object for which to write the name.
     * @param  object  the object for which to write the name.
     */
    private void appendName(final String type, final IdentifiedObject object) {
        final String name = IdentifiedObjects.getName(object, null);
        if (name != null) {
            int i      = citationLengthIndex;
            int offset = Short.toUnsignedInt(keyDirectory[i+1]);
            int length = Short.toUnsignedInt(keyDirectory[i]);
            int start  = length;
            if (citationMainKey != null) {
                final String value = citationMainKey + '=';
                asciiParams.insert(offset, value);
                length += value.length();
                citationMainKey = null;
            }
            final String value = GeoCodes.STRING_SEPARATOR + type + '=' + name;
            asciiParams.insert(offset + length, value);
            keyDirectory[i] = toShort(length += value.length());
            /*
             * After we inserted the name, adjust the offsets of all ASCII entries written after the citation.
             * Note that in the following loop, (i < limit) must be tested before increment because the limit
             * is inclusive. This loop will do nothing with GeoTIFF 1.1 because there is no other ASCII entry
             * after citation, but we keep it in case a future GeoTIFF version adds more ASCII entries.
             */
            final int shift = length - start;
            final int limit = keyCount * GeoCodes.ENTRY_LENGTH;         // Inclusive.
            i++;                                                        // Offset is the field after length.
            while (i < limit) {
                i += GeoCodes.ENTRY_LENGTH;                             // Really after (i < limit) test.
                if (keyDirectory[i-2] == (short) TAG_GEO_ASCII_PARAMS) {
                    offset = Short.toUnsignedInt(keyDirectory[i]);
                    keyDirectory[i] = toShort(offset + shift);
                }
            }
        }
    }

    /**
     * Fetches the GeoTIFF code of the given object. If {@code null}, returns {@link GeoCodes#undefined}.
     * If the object has no GeTIFF identifier, returns {@value GeoCodes#userDefined}.
     *
     * @param  type    object type: 0 = operation method, 1 = parameter.
     * @param  object  the object for which to get the GeoTIFF code.
     * @return the GeoTIFF code, or {@link GeoCodes#undefined} or {@link GeoCodes#userDefined} if none.
     * @throws FactoryException if an error occurred while fetching the GeoTIFF code.
     * @throws IncompatibleResourceException if the GeoTIFF identifier cannot be obtained.
     */
    private short getGeoCode(final int type, final IdentifiedObject object)
            throws FactoryException, IncompatibleResourceException
    {
        if (object == null) {
            return GeoCodes.undefined;
        }
        final Identifier id = IdentifiedObjects.getIdentifier(object, Citations.GEOTIFF);
        NumberFormatException cause = null;
        if (id != null) try {
            return Short.parseShort(id.getCode());
        } catch (NumberFormatException e) {
            cause = e;
        }
        throw cannotEncode(type, name(object), cause);
    }

    /**
     * Writes the EPSG code of the given object, or {@value GeoCodes#userDefined} if none.
     * Returns whether the caller should write user-defined object in replacement or in addition to EPSG code.
     *
     * @param  key     the numeric identifier of the GeoTIFF key.
     * @param  object  the object for which to get the EPSG code.
     * @return whether the caller should write user-defined object.
     * @throws FactoryException if an error occurred while fetching the EPSG code.
     */
    private boolean writeEPSG(final short key, final IdentifiedObject object) throws FactoryException {
        if (object == null) {
            writeShort(key, GeoCodes.undefined);
            missingValue(key);
            return false;
        }
        /*
         * Note `lookupEPSG(…)` will return a value only if the axes have the same order and units.
         * We could ignore axis order because GeoTIFF specification fixes it to (east, north, up),
         * but we shall not ignore axis units. The `IdentifiedObjectFinder` API does not currently
         * allow ignoring only axis order, so we fallback on strict equality (ignoring metadata).
         * This is not necessarily a bad thing, because there is a possibility that future GeoTIFF
         * specifications become stricter, so we are already "strict" regarding usages of EPSG codes.
         */
        short epsg = GeoCodes.userDefined;
        if (!disableEPSG) try {
            epsg = toShortEPSG(IdentifiedObjects.lookupEPSG(object));
        } catch (UnavailableFactoryException e) {
            listeners.warning(Level.FINE, null, e);
            disableEPSG = true;
        }
        writeShort(key, epsg);
        return (epsg == GeoCodes.userDefined);
    }

    /**
     * Returns an optional EPSG code as a short code that can be stored in a GeoTIFF key.
     *
     * @param  epsg  the optional EPSG code.
     * @return the code as a short integer, or {@link GeoCodes#userDefined} if none.
     *
     * @see #toShort(int)
     */
    private static short toShortEPSG(final Integer epsg) {
        if (epsg != null) {
            final int c = epsg;
            if (c >= 1024 && c <= 32766) {      // This range is defined by the GeoTIFF specification.
                return (short) c;
            }
        }
        return GeoCodes.userDefined;
    }

    /**
     * Appends an entry for a 16 bits integer value. This method uses a TIFF tag location of 0,
     * which implies that value is {@code SHORT}, and is contained in the "ValueOffset" entry
     *
     * @param  key    the numeric identifier of the GeoTIFF key.
     * @param  value  the value to store.
     */
    private void writeShort(final short key, final short value) {
        int i = ++keyCount * GeoCodes.ENTRY_LENGTH;
        keyDirectory[i++] = key;            // Key identifier.
        keyDirectory[++i] = 1;              // Number of values in this key.
        keyDirectory[++i] = value;          // Value offset. In this particular case, contains directly the value.
    }

    /**
     * Appends an entry for a floating point value.
     *
     * @param  key    the numeric identifier of the Key.
     * @param  value  the value to store.
     */
    private void writeDouble(final short key, final double value) {
        int i = ++keyCount * GeoCodes.ENTRY_LENGTH;
        keyDirectory[i++] = key;                                // Key identifier.
        keyDirectory[i++] = (short) TAG_GEO_DOUBLE_PARAMS;      // TIFF tag location.
        keyDirectory[i++] = 1;                                  // Number of values in this key.
        keyDirectory[i  ] = toShort(doubleCount);
        doubleParams[doubleCount++] = value;
    }

    /**
     * Appends an entry for a character string.
     *
     * @param  key    the numeric identifier of the GeoTIFF key.
     * @param  value  the value to store.
     */
    private void writeString(final short key, final String value) {
        int i = ++keyCount * GeoCodes.ENTRY_LENGTH;
        keyDirectory[i++] = key;                                // Key identifier.
        keyDirectory[i++] = (short) TAG_GEO_ASCII_PARAMS;       // TIFF tag location.
        keyDirectory[i++] = toShort(value.length());            // Number of values in this key.
        keyDirectory[i  ] = toShort(asciiParams.length());      // Offset of the first character.
        asciiParams.append(value).append(GeoCodes.STRING_SEPARATOR);
    }

    /**
     * Ensures that the given value can be represented as an unsigned 16 bits integer.
     *
     * @param  value  the value to cast to an unsigned short.
     * @return the value as an unsigned short.
     * @throws ArithmeticException if the given value cannot be stored as an unsigned 16 bits integer.
     *
     * @see #toShortEPSG(Integer)
     */
    private static short toShort(final int value) {
        if ((value & ~0xFFFF) == 0) {
            return (short) value;
        }
        throw new ArithmeticException(Errors.format(Errors.Keys.IntegerOverflow_1, Short.SIZE));
    }

    /**
     * {@return the values to write in the "GeoTIFF keys directory" tag}.
     */
    public short[] keyDirectory() {
        if (keyCount == 0) return null;
        keyDirectory[GeoCodes.ENTRY_LENGTH - 1] = (short) keyCount;
        return ArraysExt.resize(keyDirectory, (keyCount + 1) * GeoCodes.ENTRY_LENGTH);
    }

    /**
     * {@return the values to write in the "GeoTIFF double-precision parameters" tag}.
     */
    public double[] doubleParams() {
        if (doubleCount == 0) return null;
        return ArraysExt.resize(doubleParams, doubleCount);
    }

    /**
     * {@return the values to write in the "GeoTIFF ASCII strings" tag}.
     */
    public List<String> asciiParams() {
        return JDK15.isEmpty(asciiParams) ? null : List.of(asciiParams.toString());
    }

    /**
     * Returns the coefficients of the affine transform, or {@code null} if none.
     * Array length is fixed to 16 elements, for a 4×4 matrix in row-major order.
     * Axis order is fixed to (longitude, latitude, height).
     */
    public double[] modelTransformation() {
        if (gridToCRS == null) {
            return null;
        }
        /*
         * The CRS stored in GeoTIFF files have axis directions fixed to (east, north, up).
         * If the CRS of the grid geometry has different axis order, we need to adjust the
         * "grid to CRS" transform.
         */
        if (fullCRS != null) {
            final AxisDirection[] source = CoordinateSystems.getAxisDirections(fullCRS.getCoordinateSystem());
            final AxisDirection[] target = new AxisDirection[hasVerticalAxis ? 3 : 2];
            target[0] = AxisDirection.EAST;
            target[1] = AxisDirection.NORTH;
            if (hasVerticalAxis) {
                target[2] = AxisDirection.UP;
            }
            gridToCRS = Matrices.createTransform(source, target).multiply(gridToCRS);
            fullCRS   = null;       // For avoiding to do the multiplication again.
        }
        /*
         * Copy matrix coefficients. This matrix size is always 4×4, no matter the size of the `gridToCRS` matrix.
         * So we cannot invoke `MatrixSIS.getElements()`.
         */
        final double[] cf = new double[MATRIX_SIZE * MATRIX_SIZE];
        final int lastRow = gridToCRS.getNumRow() - 1;
        final int lastCol = gridToCRS.getNumCol() - 1;
        final int maxRow  = Math.min(lastRow, MATRIX_SIZE-1);
        int offset = 0;
        for (int row = 0; row < maxRow; row++) {
            copyRow(gridToCRS, row, lastCol, cf, offset);
            offset += MATRIX_SIZE;
        }
        copyRow(gridToCRS, lastRow, lastCol, cf, MATRIX_SIZE * (MATRIX_SIZE - 1));
        return cf;
    }

    /**
     * Copies a matrix row into the model transformation array.
     *
     * @param gridToCRS  the source of model transformation coefficients.
     * @param row        row of the matrix to copy.
     * @param lastCol    value of {@code gridToCRS.getNumCol() - 1}.
     * @param target     where to write the coefficients.
     * @param offset     index of the first element to write in the destination array.
     */
    private static void copyRow(final Matrix gridToCRS, final int row, final int lastCol, final double[] target, final int offset) {
        target[offset + (MATRIX_SIZE - 1)] = gridToCRS.getElement(row, lastCol);
        for (int i = Math.min(lastCol, MATRIX_SIZE-1); --i >= 0;) {
            target[offset + i] = gridToCRS.getElement(row, i);
        }
    }

    /**
     * Returns the name of the given object. Used for formatting error messages.
     *
     * @param  object  the object for which to get a name to insert in error message.
     * @return the object name.
     */
    private String name(final IdentifiedObject object) {
        return IdentifiedObjects.getDisplayName(object, listeners.getLocale());
    }

    /**
     * {@return the resources for error messages in the current locale}.
     */
    private Errors errors() {
        return Errors.forLocale(listeners.getLocale());
    }

    /**
     * {@return the resources in the current locale}.
     */
    private Resources resources() {
        return Resources.forLocale(listeners.getLocale());
    }

    /**
     * Logs a warning saying that no value is associated to the given key.
     *
     * @param  key  the GeoKey for which we found no value.
     */
    private void missingValue(final short key) {
        listeners.warning(resources().getString(Resources.Keys.MissingGeoValue_1, GeoKeys.name(key)));
    }

    /**
     * Prepares an exception saying that the given object cannot be encoded because of its type.
     *
     * @param  object  object that cannot be encoded.
     */
    private IncompatibleResourceException unsupportedType(final IdentifiedObject object) {
        String message = resources().getString(Resources.Keys.CanNotEncodeObjectType_1, ReferencingUtilities.getInterface(object));
        return new IncompatibleResourceException(message).addAspect("crs");
    }

    /**
     * Prepares an exception saying that an object of the given name cannot be encoded.
     *
     * @param  type   object type: 0 = operation method, 1 = parameter, 2 = unit of measurement.
     * @param  name   name of the object that cannot be encoded.
     * @param  cause  the reason why an error occurred, or {@code null} if none.
     */
    private IncompatibleResourceException cannotEncode(final int type, final String name, final Exception cause) {
        String message = resources().getString(Resources.Keys.CanNotEncodeNamedObject_2, type, name);
        return new IncompatibleResourceException(message, cause).addAspect("crs");
    }

    /**
     * Returns a string representation for debugging purpose.
     *
     * @return a string representation of this keys writer.
     */
    @Override
    public String toString() {
        return Strings.toString(getClass(), "citation", citation);
    }
}
