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
package org.apache.sis.storage.netcdf.base;

import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.PixelInCell;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.operation.transform.TransformSeparator;
import org.apache.sis.referencing.internal.shared.ReferencingFactoryContainer;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.storage.netcdf.internal.Resources;
import org.apache.sis.system.Modules;
import org.apache.sis.util.ArgumentChecks;
import org.apache.sis.util.iso.DefaultNameFactory;
import org.opengis.referencing.crs.SingleCRS;
import org.opengis.referencing.datum.Datum;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.NameFactory;
import org.opengis.util.NameSpace;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.DoubleStream;


/**
 * The API used internally by Apache SIS for fetching variables and attribute values from a netCDF file.
 *
 * <p>This {@code Encoder} class and subclasses are <strong>not</strong> thread-safe.
 * Synchronizations are caller's responsibility.</p>
 *
 * @author  Quentin Bialota (Geomatys)
 */
public abstract class Encoder extends ReferencingFactoryContainer {
    /**
     * The logger to use for messages other than warnings specific to the file being read.
     * This is rarely used directly because {@code listeners.getLogger()} should be preferred.
     *
     * @see #listeners
     */
    public static final Logger LOGGER = Logger.getLogger(Modules.NETCDF);

    /**
     * The format name to use in error message. We use lower-case "n" because it seems to be what the netCDF community uses.
     * By contrast, {@code NetcdfStoreProvider} uses upper-case "N" because it is considered at the beginning of sentences.
     */
    public static final String FORMAT_NAME = "netCDF";

    /**
     * The locale of data such as number formats, dates and names.
     * This is used, for example, for the conversion to lower-cases before case-insensitive searches.
     * This is not the locale for error messages or warnings reported to the user.
     */
    public static final Locale DATA_LOCALE = Locale.US;

    /**
     * The path to the netCDF file, or {@code null} if unknown.
     * This is set by netCDF store constructor and shall not be modified afterward.
     * This is used for information purpose only, not for actual reading operation.
     */
    public Path location;

    /**
     * The data store identifier created from the global attributes, or {@code null} if none.
     * Defined as a namespace for use as the scope of children resources (the variables).
     * This is set by netCDF store constructor and shall not be modified afterward.
     */
    public NameSpace namespace;

    /**
     * The factory to use for creating variable identifiers.
     */
    public final NameFactory nameFactory;

    /**
     * The library for geometric objects, or {@code null} for the default.
     * This will be used only if there is geometric objects to create.
     * If the netCDF file contains only raster data, this value is ignored.
     */
    public final GeometryLibrary geomlib;

    /**
     * The geodetic reference frame, created when first needed. The datum are generally not specified in netCDF files.
     * To make that clearer, we will build datum with names like "Unknown datum presumably based on GRS 1980".
     * Index in the cache are one of the {@code CACHE_INDEX} constants declared in {@link CRSBuilder}.
     *
     * @see CRSBuilder#build(Encoder, boolean)
     */
    final Datum[] datumCache;

    /**
     * Information for building <abbr>CRS</abbr>s and <i>grid to CRS</i> transforms for variables.
     * The {@link GridMapping} class supports different conventions: the <abbr>CF</abbr> conventions
     * are tried first, followed by <abbr>GDAL</abbr> conventions (pair of {@code "spatial_ref_sys"}
     * and {@code "GeoTransform"} attributes), then <abbr>ESRI</abbr> conventions.
     * The keys are variable names from two sources:
     *
     * <ol>
     *   <li>Name of an usually empty variable referenced by a {@code "grid_mapping"} attribute on the actual data.
     *       This is the standard approach, as it allows many variables to reference the same <abbr>CRS</abbr>
     *       definition by declaring the same value in their {@code "grid_mapping"} attribute.</li>
     *   <li>Name of the actual data variable when the attributes are found directly on that variable.
     *       This approach is non-standard, as it does not allow the sharing of the same <abbr>CRS</abbr>
     *       definition by many variables. But it is nevertheless observed in practice.</li>
     * </ol>
     *
     * @see GridMapping#forVariable(Variable)
     */
    final Map<String,GridMapping> gridMapping;

    /**
     * Cache of localization grids created for a given pair of (<var>x</var>,<var>y</var>) axes.
     * Localization grids are expensive to compute and consume a significant amount of memory.
     * The {@link Grid} instances returned by {@link #getGridCandidates()} share localization
     * grids only between variables using the exact same list of dimensions.
     * This {@code localizationGrids} cache allows to cover other cases.
     *
     * <h4>Example</h4>
     * A netCDF file may have a variable with (<var>longitude</var>, <var>latitude</var>) dimensions and another
     * variable with (<var>longitude</var>, <var>latitude</var>, <var>depth</var>) dimensions, with both variables
     * using the same localization grid for the (<var>longitude</var>, <var>latitude</var>) part.
     *
     * @see GridCacheKey#cached(Encoder)
     */
    final Map<GridCacheKey,GridCacheValue> localizationGrids;

    /**
     * Where to send the warnings.
     */
    public final StoreListeners listeners;

    /**
     * Sets to {@code true} for canceling a reading process.
     * This flag is honored on a <em>best effort</em> basis only.
     */
    public volatile boolean canceled;

    /**
     * Creates a new decoder.
     *
     * @param  geomlib    the library for geometric objects, or {@code null} for the default.
     * @param  listeners  where to send the warnings.
     */
    protected Encoder(final GeometryLibrary geomlib, final StoreListeners listeners) {
        this.geomlib      = geomlib;
        this.listeners    = Objects.requireNonNull(listeners);
        this.nameFactory  = DefaultNameFactory.provider();
        this.datumCache   = new Datum[CRSBuilder.DATUM_CACHE_SIZE];
        this.gridMapping  = new HashMap<>();
        localizationGrids = new HashMap<>();
    }

    /**
     * Builds a dimension with the given name and length.
     *
     * @param name the name of the dimension
     * @param length the length of the dimension
     * @return an array containing a single {@link DimensionInfo} object
     */
    public abstract Dimension buildDimension(final String name, int length);

    /**
     * Builds a variable with the given name, dimensions, attributes and data type.
     *
     * @param name the name of the variable
     * @param dimensions the dimensions of the variable, or {@code null} if none
     * @param attributes the attributes of the variable, or {@code null} if none
     * @param dataType the data type of the variable, or {@code null} if unknown
     * @param shape the shape of the variable
     * @param chunkShape the chunk shape of the variable, or {@code null} if unknown
     * @param data the data of the variable, or {@code null} if none
     * @param smIndex the index sample dimension in the data or {@code null} if not applicable (only used when data is a Resource)
     * @return an array containing a single {@link Variable} object
     */
    public abstract Variable buildVariable(final String name, Dimension[] dimensions,
                                           final Map<String,Object> attributes, DataType dataType, int[] shape, int[] chunkShape,
                                           Object data, Integer smIndex)
            throws DataStoreContentException;

    /**
     * Writes the given variables to the underlying output.
     *
     * @param variables the variables to write.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if a logical error occurred.
     */
    public abstract void writeVariables(List<Variable> variables) throws IOException, DataStoreException;

    /**
     * Returns a filename for formatting error message and for information purpose.
     * The filename should not contain path, but may contain file extension.
     *
     * @return a filename to include in warnings or error messages.
     */
    public abstract String getFilename();

    /**
     * Returns the netCDF-specific resource bundle for the locale given by {@link StoreListeners#getLocale()}.
     *
     * @return the localized error resource bundle.
     */
    public final Resources resources() {
        return Resources.forLocale(getLocale());
    }

    /**
     * Returns the locale used for error message, or {@code null} if unspecified.
     * In the latter case, the platform default locale will be used.
     *
     * @return the locale for messages (typically specified by the data store), or {@code null} if unknown.
     */
    @Override
    public final Locale getLocale() {
        return listeners.getLocale();
    }

    /**
     * Closes this decoder and releases resources.
     *
     * @param  lock  the lock to use in {@code synchronized(lock)} statements.
     * @throws IOException if an error occurred while closing the decoder.
     */
    public abstract void close(DataStore lock) throws IOException;

    /**
     * Find all spatial points available for a single dimension.
     * @implNote :
     * <ul>
     * <li>Require 1D system as input.</li>
     * <li>do NOT work when queried dimension index is flipped between grid
     * and spatial envelope.</li>
     * <li>Use an approach which works with non-linear axes : find all grid
     * steps, then reproject them all to find dimension values.</li>
     * </ul>
     * @param dimOfInterest Dimension to get spatial points for.
     * @return An ordered list (in grid order) of available values.
     * @throws IllegalArgumentException if given CRS is not a single dimension one.
     * @throws TransformException If we cannot transform grid coordinate into
     * spatial ones.
     */
    protected static double[] getPositions(final SingleCRS dimOfInterest, GridGeometry geom) throws DataStoreException, TransformException {
        ArgumentChecks.ensureNonNull("Dimension of interest", dimOfInterest);
        ArgumentChecks.ensureDimensionMatches("Dimension of interest", 1, dimOfInterest);
        int dimIdx = 0;
        for (SingleCRS part : CRS.getSingleComponents(geom.getCoordinateReferenceSystem())) {
            if (part == dimOfInterest) break;
            dimIdx += part.getCoordinateSystem().getDimension();
        }

        final MathTransform gridToCRS = geom.getGridToCRS(PixelInCell.CELL_CENTER);
        final TransformSeparator sep = new TransformSeparator(gridToCRS);
        sep.addSourceDimensions(dimIdx);
        sep.addTargetDimensions(dimIdx);
        final GridExtent extent = geom.getExtent();
        final int dimGridSpan = Math.toIntExact(extent.getSize(dimIdx));
        try {
            final MathTransform targetTransform = sep.separate();
            final double[] gridPoints = DoubleStream.iterate(extent.getLow(dimIdx), i -> i + 1)
                    .limit(dimGridSpan)
                    .toArray();
            final double[] axisValues = new double[gridPoints.length];
            targetTransform.transform(gridPoints, 0, axisValues, 0, gridPoints.length);
            return axisValues;
        } catch (Exception e) {
            // Fallback on costly approach : project entire grid points
            final int nDims = extent.getDimension();
            final double[] values = new double[dimGridSpan];
            final double[] buffer = new double[nDims];

            // Set all dims to grid lower bound
            for (int j = 0; j < nDims; j++) {
                buffer[j] = extent.getLow(j);
            }
            // Sweep along dimension of interest
            for (int i = 0; i < dimGridSpan; i++) {
                buffer[dimIdx] = extent.getLow(dimIdx) + i;
                gridToCRS.transform(buffer, 0, buffer, 0, 1);
                values[i] = buffer[dimIdx];
            }
            return values;
        }
    }
}
