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
package org.apache.sis.coverage.grid;

import java.util.Arrays;
import java.util.Locale;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.RoundingMode;
import org.opengis.geometry.Envelope;
import org.opengis.geometry.DirectPosition;
import org.opengis.util.FactoryException;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.Matrix;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.referencing.operation.transform.LinearTransform;
import org.apache.sis.referencing.operation.transform.TransformSeparator;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.referencing.CRS;
import org.apache.sis.internal.referencing.WraparoundAdjustment;
import org.apache.sis.internal.referencing.DirectPositionView;
import org.apache.sis.geometry.GeneralDirectPosition;
import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.geometry.Envelopes;
import org.apache.sis.internal.feature.Resources;
import org.apache.sis.util.resources.Vocabulary;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.util.ArgumentChecks;
import org.apache.sis.util.ArraysExt;
import org.apache.sis.util.CharSequences;
import org.apache.sis.util.Classes;
import org.apache.sis.util.Debug;
import org.apache.sis.util.collection.DefaultTreeTable;
import org.apache.sis.util.collection.TableColumn;
import org.apache.sis.util.collection.TreeTable;
import org.apache.sis.util.StringBuilders;
import org.apache.sis.math.MathFunctions;

// Branch-dependent imports
import org.opengis.coverage.PointOutsideCoverageException;


/**
 * Creates a new grid geometry derived from a base grid geometry with different extent or resolution.
 * {@code GridDerivation} are created by calls to {@link GridGeometry#derive()}.
 * Properties of the desired grid geometry can be specified by calls to the following methods,
 * in that order (each method is optional):
 *
 * <ol>
 *   <li>{@link #rounding(GridRoundingMode)}, {@link #margin(int...)} and/or {@link #chunkSize(int...)} in any order</li>
 *   <li>{@link #subgrid(GridGeometry)}, {@link #subgrid(Envelope, double...)} or {@link #subgrid(GridExtent, int...)}</li>
 *   <li>{@link #slice(DirectPosition)} and/or {@link #sliceByRatio(double, int...)}</li>
 * </ol>
 *
 * Then the grid geometry is created by a call to {@link #build()}.
 * The {@link #getIntersection()} method can also be invoked for the {@link GridExtent} part without subsampling.
 *
 * <p>All methods in this class preserve the number of dimensions. For example the {@link #slice(DirectPosition)} method sets
 * the {@linkplain GridExtent#getSize(int) grid size} to 1 in all dimensions specified by the <cite>slice point</cite>,
 * but does not remove those dimensions from the grid geometry.
 * For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</p>
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 *
 * @see GridGeometry#derive()
 * @see GridGeometry#reduce(int...)
 *
 * @since 1.0
 * @module
 */
public class GridDerivation {
    /**
     * The base grid geometry from which to derive a new grid geometry.
     */
    protected final GridGeometry base;

    /**
     * Controls behavior of rounding from floating point values to integers.
     *
     * @see #rounding(GridRoundingMode)
     */
    private GridRoundingMode rounding;

    /**
     * Whether to clip the derived grid extent to the original grid extent.
     *
     * @see #clipping(GridClippingMode)
     */
    private GridClippingMode clipping;

    /**
     * If non-null, the extent will be expanded by that amount of cells on each grid dimension.
     * This array is non-null only if at least one non-zero margin has been specified. Trailing
     * zero values are omitted (consequently this array may be shorter than {@link GridExtent}
     * number of dimensions).
     *
     * @see #margin(int...)
     */
    private int[] margin;

    /**
     * If the grid is divided in tiles or chunks, the size of the chunks.
     * This is used for snapping grid size to multiple values of chunk size.
     *
     * @see #chunkSize(int...)
     */
    private int[] chunkSize;

    // ──────── FIELDS COMPUTED BY METHODS IN THIS CLASS ──────────────────────────────────────────────────────────────

    /**
     * Tells whether the {@link #baseExtent} has been expanded by addition of {@linkplain #margin} and rounding
     * to {@linkplain #chunkSize chunk size}. We have this flag because it is not always convenient to add margin
     * immediately, depending on how the {@link #baseExtent} has been updated.
     *
     * @see #getBaseExtentExpanded()
     */
    private boolean isBaseExtentExpanded;

    /**
     * The sub-extent of {@link #base} grid geometry to use for the new grid geometry. This is the intersection
     * of {@code base.extent} with any area of interest specified to a {@code subgrid(…)} method,
     * potentially with some grid size set to 1 by a {@link #slice(DirectPosition)} method.
     * This extent is <strong>not</strong> scaled or subsampled for a given resolution.
     * It <strong>may</strong> be expanded according the {@link #margin} and {@link #chunkSize} values,
     * depending on whether the {@link #isBaseExtentExpanded} flag value is {@code true}.
     *
     * <p>This extent is initialized to {@code base.extent} if no slice, scale or sub-grid has been requested.
     * This field may be {@code null} if the base grid geometry does not define any extent.
     * A successful call to {@link GridGeometry#requireGridToCRS(boolean)} guarantees that this field is non-null.</p>
     *
     * @see #getIntersection()
     */
    private GridExtent baseExtent;

    /**
     * Same as {@link #baseExtent} (expanded), but takes resolution or subsampling in account.
     * This is {@code null} if no scale or subsampling has been applied.
     * {@linkplain #margin Margin} and {@linkplain #chunkSize chunk size}
     * shall be applied before {@code scaledExtent} is computed.
     *
     * @todo if a {@linkplain #margin} has been specified, then we need to perform an additional clipping.
     */
    private GridExtent scaledExtent;

    /**
     * The conversion from the derived grid to the original grid, or {@code null} if no subsampling is applied.
     * A non-null conversion exists only in case of subsampling,
     * because otherwise the derived grid shares the same coordinate space than the {@linkplain #base} grid.
     * If non-null, the transform has the following properties:
     *
     * <ul>
     *   <li>The transform has no shear, no rotation, no axis flip.</li>
     *   <li>Scale factors on the diagonal are the {@linkplain #getSubsampling() subsampling} values.
     *       Those values are strictly positive integers, except if computed by {@link #subgrid(Envelope, double...)}.</li>
     *   <li>Translation terms in the last column are integers between 0 inclusive and subsampling factors exclusive.
     *       Those values are positive integers, except if computed by {@link #subgrid(Envelope, double...)}.</li>
     * </ul>
     *
     * This transform maps {@linkplain PixelInCell#CELL_CORNER pixel corners}.
     *
     * @see #getSubsampling()
     * @see #getSubsamplingOffsets()
     */
    private LinearTransform toBase;

    /**
     * List of grid dimensions that are modified by the {@code cornerToCRS} transform, or null for all dimensions.
     * The length of this array is the number of dimensions of the given Area Of Interest (AOI). Each value in this
     * array is between 0 inclusive and {@code extent.getDimension()} exclusive. This is a temporary information
     * set by {@link #dropUnusedDimensions(MathTransform, int)} and cleared when no longer needed.
     */
    private int[] modifiedDimensions;

    /**
     * If {@link #subgrid(Envelope, double...)} or {@link #slice(DirectPosition)} has been invoked, the method name.
     * This is used for preventing those methods to be invoked twice or out-of-order, which is currently not supported.
     */
    private String subGridSetter;

    /**
     * Intersection between the grid envelope and the area of interest, computed when only envelopes are available.
     * Normally we do not compute this envelope directly; instead we compute the grid extent and the "grid to CRS"
     * transform. This envelope is computed only if it can not be computed from other grid geometry properties.
     *
     * @see #subgrid(Envelope, double...)
     */
    private GeneralEnvelope intersection;

    /**
     * Creates a new builder for deriving a grid geometry from the specified base.
     *
     * @param  base  the base to use as a template for deriving a new grid geometry.
     *
     * @see GridGeometry#derive()
     */
    protected GridDerivation(final GridGeometry base) {
        ArgumentChecks.ensureNonNull("base", base);
        this.base  = base;
        baseExtent = base.extent;                    // May be null.
        rounding   = GridRoundingMode.NEAREST;
        clipping   = GridClippingMode.STRICT;
    }

    /**
     * Verifies that a sub-grid has not yet been defined.
     * This method is invoked for enforcing the method call order defined in javadoc.
     */
    private void ensureSubgridNotSet() {
        if (subGridSetter != null) {
            throw new IllegalStateException(Resources.format(Resources.Keys.CanNotSetDerivedGridProperty_1, subGridSetter));
        }
    }

    /**
     * Controls behavior of rounding from floating point values to integers.
     * This setting modifies computations performed by the following methods
     * (it has no effect on other methods in this {@code GridDerivation} class):
     * <ul>
     *   <li>{@link #slice(DirectPosition)}</li>
     *   <li>{@link #subgrid(Envelope, double...)}</li>
     * </ul>
     *
     * If this method is never invoked, the default value is {@link GridRoundingMode#NEAREST}.
     * If this method is invoked too late, an {@link IllegalStateException} is thrown.
     *
     * @param  mode  the new rounding mode.
     * @return {@code this} for method call chaining.
     * @throws IllegalStateException if {@link #subgrid(Envelope, double...)} or {@link #slice(DirectPosition)}
     *         has already been invoked.
     */
    public GridDerivation rounding(final GridRoundingMode mode) {
        ArgumentChecks.ensureNonNull("mode", mode);
        ensureSubgridNotSet();
        rounding = mode;
        return this;
    }

    /**
     * Specifies whether to clip the derived grid extent to the extent of the base grid geometry.
     * The default value is {@link GridClippingMode#STRICT}.
     *
     * @param  mode  whether to clip the derived grid extent.
     * @return {@code this} for method call chaining.
     * @throws IllegalStateException if {@link #subgrid(Envelope, double...)} or {@link #slice(DirectPosition)}
     *         has already been invoked.
     *
     * @since 1.1
     */
    public GridDerivation clipping(final GridClippingMode mode) {
        ArgumentChecks.ensureNonNull("mode", mode);
        ensureSubgridNotSet();
        clipping = mode;
        return this;
    }

    /**
     * Specifies an amount of cells by which to expand {@code GridExtent} after rounding.
     * This setting modifies computations performed by the following methods:
     * <ul>
     *   <li>{@link #subgrid(GridGeometry)}</li>
     *   <li>{@link #subgrid(Envelope, double...)}</li>
     *   <li>{@link #subgrid(GridExtent, int...)}</li>
     * </ul>
     *
     * For each dimension <var>i</var> of the grid computed by above methods, the {@linkplain GridExtent#getLow(int) low}
     * grid coordinate is subtracted by {@code cellCount[i]} and the {@linkplain GridExtent#getHigh(int) high}
     * grid coordinate is increased  by {@code cellCount[i]}.
     * This calculation is done in units of the {@linkplain #base} grid cells, i.e. before subsampling.
     * For example if subsampling is 2, then a margin of 6 cells specified with this method will result
     * in a margin of 3 cells in the grid extent computed by the {@link #build()} method.
     *
     * <div class="note"><b>Use case:</b>
     * if the caller wants to apply bilinear interpolations in an image, (s)he will need 1 more pixel on each image border.
     * If the caller wants to apply bi-cubic interpolations, (s)he will need 2 more pixels on each image border.</div>
     *
     * If this method is never invoked, the default value is zero for all dimensions.
     * If this method is invoked too late, an {@link IllegalStateException} is thrown.
     * If the {@code cellCounts} array length is shorter than the grid dimension,
     * then zero is assumed for all missing dimensions.
     *
     * @param  cellCounts  number of cells by which to expand the grid extent.
     * @return {@code this} for method call chaining.
     * @throws IllegalArgumentException if a value is negative.
     * @throws IllegalStateException if {@link #subgrid(Envelope, double...)} or {@link #slice(DirectPosition)}
     *         has already been invoked.
     *
     * @see GridExtent#expand(long...)
     */
    public GridDerivation margin(final int... cellCounts) {
        ArgumentChecks.ensureNonNull("cellCounts", cellCounts);
        ensureSubgridNotSet();
        int[] margin = null;
        for (int i=cellCounts.length; --i >= 0;) {
            final int n = cellCounts[i];
            if (n != 0) {
                ArgumentChecks.ensurePositive("cellCounts", n);
                if (margin == null) {
                    margin = new int[i+1];
                }
                margin[i] = n;
            }
        }
        this.margin = margin;           // Set only on success. We want null if all margin values are 0.
        return this;
    }

    /**
     * Specifies the size of tiles or chunks in the base grid geometry. If a chunk size is specified,
     * then the grid extent computed by {@link #build()} will span an integer amount of chunks.
     * The grid coordinates (0, 0, …) locate the corner of a chunk.
     *
     * <p>This property operates on the same methods than the {@linkplain #margin(int...) margin}.
     * If both a margin and a chunk size are specified, then margins are added first
     * and the resulting grid coordinates are rounded to chunk size.
     * This calculation is done in units of the {@linkplain #base} grid cells, i.e. before subsampling.
     * For example if subsampling is 2, then a tile size of 20×20 pixels specified with this method will
     * result in a tile size of 10×10 cells in the grid extent computed by the {@link #build()} method.</p>
     *
     * <p>If this method is never invoked, the default value is one for all dimensions.
     * If this method is invoked too late, an {@link IllegalStateException} is thrown.
     * If the {@code cellCounts} array length is shorter than the grid dimension,
     * then one is assumed for all missing dimensions.</p>
     *
     * @param  cellCounts  number of cells in all tiles or chunks.
     * @return {@code this} for method call chaining.
     * @throws IllegalArgumentException if a value is zero or negative.
     * @throws IllegalStateException if {@link #subgrid(Envelope, double...)} or {@link #slice(DirectPosition)}
     *         has already been invoked.
     *
     * @since 1.1
     */
    public GridDerivation chunkSize(final int... cellCounts) {
        ArgumentChecks.ensureNonNull("cellCounts", cellCounts);
        ensureSubgridNotSet();
        int[] chunkSize = null;
        for (int i=cellCounts.length; --i >= 0;) {
            final int n = cellCounts[i];
            if (n != 1) {
                ArgumentChecks.ensureStrictlyPositive("cellCounts", n);
                if (chunkSize == null) {
                    chunkSize = new int[i+1];
                    Arrays.fill(chunkSize, 1);
                }
                chunkSize[i] = n;
            }
        }
        this.chunkSize = chunkSize;         // Set only on success. We want null if all size values are 1.
        return this;
    }

    /**
     * Requests a grid geometry where cell sizes have been scaled by the given factors, which result in a change of grid size.
     * The new grid geometry is given a <cite>"grid to CRS"</cite> transform computed as the concatenation of given scale factors
     * (applied on grid indices) followed by the {@linkplain GridGeometry#getGridToCRS(PixelInCell) grid to CRS} transform of the
     * grid geometry specified at construction time. The resulting grid extent can be specified explicitly (typically as an extent
     * computed by {@link GridExtent#resize(long...)}) or computed automatically by this method.
     *
     * <div class="note"><b>Example:</b>
     * if the original grid geometry had an extent of [0 … 5] in <var>x</var> and [0 … 8] in <var>y</var>, then a call to
     * {@code resize(null, 0.1, 0.1)} will build a grid geometry with an extent of [0 … 50] in <var>x</var> and [0 … 80] in <var>y</var>.
     * This new extent covers the same geographic area than the old extent but with pixels having a size of 0.1 times the old pixels size.
     * The <cite>grid to CRS</cite> transform of the new grid geometry will be pre-concatenated with scale factors of 0.1 in compensation
     * for the shrink in pixels size.</div>
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>This method can be invoked only once.</li>
     *   <li>This method can not be used together with a {@code subgrid(…)} method.</li>
     *   <li>If a non-default rounding mode or a non-default clipping mode is desired,
     *       they should be {@linkplain #rounding(GridRoundingMode) specified} before to invoke this method.</li>
     *   <li>If the grid extent is recomputed by this method, then the {@linkplain #margin(int...) margin} and
     *       {@linkplain #chunkSize(int...)} will be taken in account.</li>
     *   <li>This method does not reduce the number of dimensions of the grid geometry.
     *       For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</li>
     * </ul>
     *
     * The {@code scales} parameter in this method can be seen as an alternative to the {@code resolution} parameter
     * in {@link #subgrid(Envelope, double...)} working in grid coordinates space instead of CRS coordinates space.
     *
     * @param  extent  the grid extent to set as a result of the given scale, or {@code null} for computing it automatically.
     *                 If non-null, then this given extent is used <i>as-is</i> without checking intersection with the base
     *                 grid geometry.
     * @param  scales  the scale factors to apply on grid indices. If the length of this array is smaller than the number of
     *                 grid dimension, then a scale of 1 is assumed for all missing dimensions.
     * @return {@code this} for method call chaining.
     * @throws IllegalStateException if a {@link #subgrid(GridGeometry) subgrid(…)} or {@link #slice(DirectPosition) slice(…)}
     *         method has already been invoked.
     *
     * @see #subsample(int...)
     * @see GridExtent#resize(long...)
     *
     * @deprecated This method does not handle margin, chunk size and clipping according their contract.
     *             It is replaced by {@link #subgrid(GridExtent, int...)}.
     */
    @Deprecated
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public GridDerivation resize(GridExtent extent, double... scales) {
        ensureSubgridNotSet();
        ArgumentChecks.ensureNonNull("scales", scales);
        base.getGridToCRS(PixelInCell.CELL_CENTER);             // For making sure that the transform exist.
        final int n = base.getDimension();
        if (extent != null) {
            final int actual = extent.getDimension();
            if (actual != n) {
                throw new IllegalArgumentException(Errors.format(
                        Errors.Keys.MismatchedDimension_3, "extent", n, actual));
            }
        }
        subGridSetter = "resize";
        /*
         * Computes the affine transform to pre-concatenate with the `gridToCRS` transform.
         * This is the simplest calculation done in this class since we are already in grid coordinates.
         * The given `scales` array will become identical to `this.scales` after length adjustment.
         */
        final int actual = scales.length;
        scales = Arrays.copyOf(scales, n);
        if (actual < n) {
            Arrays.fill(scales, actual, n, 1);
        }
        toBase = MathTransforms.scale(scales);
        /*
         * If the user did not specified explicitly the resulting grid extent, compute it now.
         * This operation should never fail since we use a known implementation of MathTransform,
         * unless some of the given scale factors were too close to zero.
         */
        if (extent == null && baseExtent != null) try {
            final LinearTransform mt = toBase.inverse();
            final GeneralEnvelope indices = baseExtent.toCRS(mt, mt, null);
            extent = new GridExtent(indices, rounding, clipping, margin, chunkSize, null, null);
            // PROBLEM! `margin` and `chunkSize` are not in `scaledExtent` unit.
        } catch (TransformException e) {
            throw new IllegalArgumentException(e);
        }
        scaledExtent = extent;
        // Note: current version does not update `baseExtent`.
        return this;
    }

    /**
     * Adapts the base grid for the geographic area and resolution of the given grid geometry.
     * The new grid geometry will cover the spatiotemporal region given by {@code areaOfInterest} envelope
     * (coordinate operations are applied as needed if the Coordinate Reference Systems are not the same).
     * The new grid geometry resolution will be integer multiples of the {@link #base} grid geometry resolution.
     *
     * <div class="note"><b>Usage:</b>
     * This method can be helpful for implementation of
     * {@link org.apache.sis.storage.GridCoverageResource#read(GridGeometry, int...)}.
     * Example:
     *
     * {@preformat java
     *     class MyDataStorage extends GridCoverageResource {
     *         &#64;Override
     *         public GridCoverage read(GridGeometry domain, int... range) throws DataStoreException {
     *             GridDerivation change = getGridGeometry().derive().subgrid(domain);
     *             GridExtent toRead = change.buildExtent();
     *             int[] subsampling = change.getSubsampling());
     *             // Do reading here.
     *         }
     *     }
     * }
     * </div>
     *
     * If {@code gridExtent} contains only an envelope, then this method delegates to {@link #subgrid(Envelope, double...)}.
     * Otherwise if {@code gridExtent} contains only an extent, then this method delegates to {@link #subgrid(GridExtent, int...)}.
     * Otherwise the following information are mandatory:
     * <ul>
     *   <li>{@linkplain GridGeometry#getExtent() Extent} in {@code areaOfInterest}.</li>
     *   <li>{@linkplain GridGeometry#getGridToCRS(PixelInCell) Grid to CRS} conversion in {@code areaOfInterest}.</li>
     *   <li>{@linkplain GridGeometry#getGridToCRS(PixelInCell) Grid to CRS} conversion in {@link #base} grid.</li>
     * </ul>
     *
     * The following information are optional but recommended:
     * <ul>
     *   <li>{@linkplain GridGeometry#getCoordinateReferenceSystem() Coordinate reference system} in {@code areaOfInterest}.</li>
     *   <li>{@linkplain GridGeometry#getCoordinateReferenceSystem() Coordinate reference system} in {@link #base} grid.</li>
     *   <li>{@linkplain GridGeometry#getExtent() Extent} in {@link #base} grid.</li>
     * </ul>
     *
     * Optional {@linkplain #margin(int...) margin} and {@linkplain #chunkSize(int...) chunk size} can be specified
     * for increasing the size of the grid extent computed by this method. For example if the caller wants to apply
     * bilinear interpolations in an image, (s)he will need 1 more pixel on each image border.
     * If the caller wants to apply bi-cubic interpolations, (s)he will need 2 more pixels on each image border.
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>This method can be invoked only once.</li>
     *   <li>This method can not be used together with another {@code subgrid(…)} method.</li>
     *   <li>{@linkplain #rounding(GridRoundingMode) Rounding mode}, {@linkplain #clipping(GridClippingMode) clipping mode},
     *       {@linkplain #margin(int...) margin} and {@linkplain #chunkSize(int...) chunk size},
     *       if different than default values, should be set before to invoke this method.</li>
     *   <li>{@linkplain #slice(DirectPosition) Slicing} can be applied after this method.</li>
     *   <li>This method does not reduce the number of dimensions of the grid geometry.
     *       For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</li>
     * </ul>
     *
     * @param  areaOfInterest  the area of interest and desired resolution as a grid geometry.
     * @return {@code this} for method call chaining.
     * @throws DisjointExtentException if the given grid of interest does not intersect the grid extent.
     * @throws IncompleteGridGeometryException if a mandatory property of a grid geometry is absent.
     * @throws IllegalGridGeometryException if an error occurred while converting the envelope coordinates to grid coordinates.
     * @throws IllegalStateException if a {@link #subgrid(Envelope, double...) subgrid(…)} or {@link #slice(DirectPosition) slice(…)}
     *         method has already been invoked.
     *
     * @see #getIntersection()
     * @see #getSubsampling()
     */
    public GridDerivation subgrid(final GridGeometry areaOfInterest) {
        ensureSubgridNotSet();
        ArgumentChecks.ensureNonNull("areaOfInterest", areaOfInterest);
        if (areaOfInterest.isEnvelopeOnly()) {
            return subgrid(areaOfInterest.envelope, (double[]) null);
        }
        if (areaOfInterest.isExtentOnly()) {
            int[] subsampling = null;
            if (areaOfInterest.resolution != null) {
                /*
                 * In principle `resolution` is always null here because it is computed from `gridToCRS`,
                 * which is null (otherwise `isExtentOnly()` would have been false). However an exception
                 * to this rule happens if `areaOfInterest` has been computed by another `GridDerivation`,
                 * in which case the resolution requested by user is saved even when `gridToCRS` is null.
                 * In that case the resolution is relative to the base grid of the other `GridDerivation`.
                 * Note however that the `resolution` field is only an approximation (the exact transform
                 * would have been stored in `gridToCRS` if it was non-null) and the subsampling offsets
                 * are lost (they would also have been stored in `gridToCRS`).
                 */
                subsampling = new int[areaOfInterest.resolution.length];
                for (int i=0; i<subsampling.length; i++) {
                    subsampling[i] = roundSubsampling(areaOfInterest.resolution[i], i);
                }
            }
            return subgrid(areaOfInterest.extent, subsampling);
        }
        subGridSetter = "subgrid";
        if (base.equals(areaOfInterest)) {
            return this;
        }
        final MathTransform mapCenters;
        final GridExtent domain = areaOfInterest.getExtent();       // May throw IncompleteGridGeometryException.
        try {
            final CoordinateOperationFinder finder = new CoordinateOperationFinder(areaOfInterest, base);
            final MathTransform mapCorners = finder.gridToGrid();
            finder.setAnchor(PixelInCell.CELL_CENTER);
            finder.nowraparound();
            mapCenters = finder.gridToGrid();                               // We will use only the scale factors.
            setBaseExtentClipped(domain.toCRS(mapCorners, mapCenters, null));
        } catch (FactoryException | TransformException e) {
            throw new IllegalGridGeometryException(e, "areaOfInterest");
        }
        if (baseExtent != base.extent && baseExtent.equals(areaOfInterest.extent)) {
            baseExtent = areaOfInterest.extent;                                         // Share common instance.
        }
        /*
         * The subsampling will determine the scale factors in the transform from the given desired grid geometry
         * to the `base` grid geometry. For example a scale of 10 means that every time we advance by one pixel in
         * `areaOfInterest`, we will advance by 10 pixels in `base`.  We compute the scales (indirectly because of
         * the way transforms are concatenated) as the ratio between the resolutions of the `areaOfInterest` and
         * `base` grid geometries, computed in the center of the area of interest.
         */
        // The `domain` extent must be the source of the `mapCenters` transform.
        final double[] scales = GridGeometry.resolution(mapCenters, domain);
        if (scales == null) {
            return this;
        }
        final int[] subsampling = new int[scales.length];
        for (int i=0; i<subsampling.length; i++) {
            subsampling[i] = roundSubsampling(scales[i], i);
        }
        return subsample(subsampling);
    }

    /**
     * Requests a grid geometry over a sub-envelope and optionally with a different a coarser resolution.
     * The given envelope does not need to be expressed in the same coordinate reference system (CRS)
     * than {@linkplain GridGeometry#getCoordinateReferenceSystem() the CRS of the base grid geometry};
     * coordinate conversions or transformations will be applied as needed.
     * That envelope CRS may have fewer dimensions than the base grid geometry CRS,
     * in which case grid dimensions not mapped to envelope dimensions will be returned unchanged.
     * The target resolution, if provided, shall be in same units and same order than the given envelope axes.
     * If the length of {@code resolution} array is less than the number of dimensions of {@code areaOfInterest},
     * then no subsampling will be applied on the missing dimensions.
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>This method can be invoked only once.</li>
     *   <li>This method can not be used together with another {@code subgrid(…)} method.</li>
     *   <li>{@linkplain #rounding(GridRoundingMode) Rounding mode}, {@linkplain #clipping(GridClippingMode) clipping mode},
     *       {@linkplain #margin(int...) margin} and {@linkplain #chunkSize(int...) chunk size},
     *       if different than default values, should be set before to invoke this method.</li>
     *   <li>{@linkplain #slice(DirectPosition) Slicing} can be applied after this method.</li>
     *   <li>This method does not reduce the number of dimensions of the grid geometry.
     *       For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</li>
     *   <li>If the given envelope is known to be expressed in the same CRS than the grid geometry,
     *       then the {@linkplain Envelope#getCoordinateReferenceSystem() CRS of the envelope}
     *       can be left unspecified ({@code null}). It may give a slight performance improvement
     *       by avoiding the check for coordinate transformation.</li>
     *   <li>Subsampling computed by this method may be fractional. Consequently calls to {@link #getSubsampling()} and
     *       {@link #getSubsamplingOffsets()} after this method may cause an {@link IllegalStateException} to be thrown.</li>
     * </ul>
     *
     * @param  areaOfInterest  the desired spatiotemporal region in any CRS (transformations will be applied as needed),
     *                         or {@code null} for not restricting the sub-grid to a sub-area.
     * @param  resolution      the desired resolution in the same units and order than the axes of the given envelope,
     *                         or {@code null} or an empty array if no subsampling is desired. The array length should
     *                         be equal to the {@code areaOfInterest} dimension, but this is not mandatory
     *                         (zero or missing values mean no sub-sampling, extraneous values are ignored).
     * @return {@code this} for method call chaining.
     * @throws DisjointExtentException if the given area of interest does not intersect the grid extent.
     * @throws IncompleteGridGeometryException if the base grid geometry has no extent, no "grid to CRS" transform,
     *         or no CRS (unless {@code areaOfInterest} has no CRS neither, in which case the CRS are assumed the same).
     * @throws IllegalGridGeometryException if an error occurred while converting the envelope coordinates to grid coordinates.
     * @throws IllegalStateException if a {@link #subgrid(GridGeometry) subgrid(…)} or {@link #slice(DirectPosition) slice(…)}
     *         method has already been invoked.
     *
     * @see #getIntersection()
     * @see #getSubsampling()
     */
    public GridDerivation subgrid(Envelope areaOfInterest, double... resolution) {
        ensureSubgridNotSet();
        final boolean isEnvelopeOnly = base.isEnvelopeOnly() && (resolution == null || resolution.length == 0);
        MathTransform cornerToCRS = isEnvelopeOnly ? MathTransforms.identity(base.envelope.getDimension())
                                                   : base.requireGridToCRS(false);         // Normal case.
        subGridSetter = "subgrid";
        try {
            /*
             * If the envelope CRS is different than the expected CRS, concatenate the envelope transformation
             * to the `gridToCRS` transform.  We should not transform the envelope here - only concatenate the
             * transforms - because transforming envelopes twice would add errors.
             */
            MathTransform baseToAOI = null;
            if (areaOfInterest != null) {
                final CoordinateReferenceSystem crs = areaOfInterest.getCoordinateReferenceSystem();
                if (crs != null) {
                    areaOfInterest = new DimensionReducer(base, crs).apply(areaOfInterest);
                    CoordinateOperation op = Envelopes.findOperation(base.envelope, areaOfInterest);
                    if (op == null) {
                        /*
                         * If above call to `Envelopes.findOperation(…)` failed, then `base.envelope` CRS is probably null.
                         * Try with a call to `getCoordinateReferenceSystem()` for throwing IncompleteGridGeometryException,
                         * unless the user overrode that method in which case we will use its value.
                         */
                        op = CRS.findOperation(base.getCoordinateReferenceSystem(), crs, null);
                    }
                    baseToAOI = op.getMathTransform();
                    cornerToCRS = MathTransforms.concatenate(cornerToCRS, baseToAOI);
                }
            }
            /*
             * If the grid geometry contains only an envelope, and if user asked nothing more than intersecting
             * envelopes, then we will return a new GridGeometry with that intersection and nothing else.
             */
            if (isEnvelopeOnly) {
                if (areaOfInterest != null) {
                    intersection = new GeneralEnvelope(base.envelope);
                    if (baseToAOI != null && !baseToAOI.isIdentity()) {
                        areaOfInterest = Envelopes.transform(baseToAOI.inverse(), areaOfInterest);
                    }
                    intersection.intersect(areaOfInterest);
                }
                return this;
            }
            /*
             * If the envelope dimensions do not encompass all grid dimensions, the transform is probably non-invertible.
             * We need to reduce the number of grid dimensions in the transform for having a one-to-one relationship.
             */
            int dimension = cornerToCRS.getTargetDimensions();
            ArgumentChecks.ensureDimensionMatches("areaOfInterest", dimension, areaOfInterest);
            cornerToCRS = dropUnusedDimensions(cornerToCRS, dimension);
            /*
             * Compute the sub-extent for the given Area Of Interest (AOI), ignoring for now the subsampling.
             * If no area of interest has been specified, or if the result is identical to the original extent,
             * then we will keep the reference to the original GridExtent (i.e. we share existing instances).
             */
            dimension = baseExtent.getDimension();      // Non-null since `base.requireGridToCRS()` succeed.
            GeneralEnvelope indices = null;
            if (areaOfInterest != null) {
                indices = new WraparoundAdjustment(base.envelope, baseToAOI, cornerToCRS.inverse()).shift(areaOfInterest);
                setBaseExtentClipped(indices);
            }
            if (indices == null || indices.getDimension() != dimension) {
                indices = new GeneralEnvelope(dimension);
            }
            final GridExtent extent = getBaseExtentExpanded(true);
            for (int i=0; i<dimension; i++) {
                long high = extent.getHigh(i);
                if (high != Long.MAX_VALUE) high++;                 // Increment before conversion to `double`.
                indices.setRange(i, extent.getLow(i), high);
            }
            /*
             * Convert the target resolutions to grid cell subsampling and adjust the extent consequently.
             * We perform this conversion by handling the resolutions as a small translation vector located
             * at the point of interest, and converting it to a translation vector in grid coordinates. The
             * conversion is done by a multiplication with the "CRS to grid" derivative at that point.
             *
             * The subsampling will be rounded in such a way that the difference in grid size is less than
             * one half of cell. Demonstration:
             *
             *    e = Math.getExponent(span)     →    2^e ≤ span
             *    a = e+1                        →    2^a > span     →    1/2^a < 1/span
             *   Δs = (s - round(s)) / 2^a
             *   (s - round(s)) ≤ 0.5            →    Δs  ≤  0.5/2^a  <  0.5/span
             *   Δs < 0.5/span                   →    Δs⋅span < 0.5 cell.
             */
            if (resolution != null && resolution.length != 0) {
                resolution = ArraysExt.resize(resolution, cornerToCRS.getTargetDimensions());
                Matrix affine = cornerToCRS.derivative(new DirectPositionView.Double(getPointOfInterest()));
                final double[] subsampling = Matrices.inverse(affine).multiply(resolution);
                final int[] modifiedDimensions = this.modifiedDimensions;                   // Will not change anymore.
                boolean scaled = false;
                for (int k=0; k < subsampling.length; k++) {
                    double s = Math.abs(subsampling[k]);
                    if (s > 1) {                                // Also for skipping NaN values.
                        scaled = true;
                        final int i = (modifiedDimensions != null) ? modifiedDimensions[k] : k;
                        final int accuracy = Math.max(0, Math.getExponent(indices.getSpan(i))) + 1;     // Power of 2.
                        s = Math.scalb(Math.rint(Math.scalb(s, accuracy)), -accuracy);
                        indices.setRange(i, indices.getLower(i) / s,
                                            indices.getUpper(i) / s);
                    }
                    subsampling[k] = s;
                }
                /*
                 * If at least one subsampling is effective, build a scale from the old grid coordinates to the new
                 * grid coordinates. If we had no rounding, the conversion would be only a scale. But because of rounding,
                 * we need a small translation for the difference between the "real" coordinate and the integer coordinate.
                 *
                 * TODO: need to clip to baseExtent, taking in account the difference in resolution.
                 */
                if (scaled) {
                    /*
                     * The `margin` and `chunkSize` arguments must be null because `scaledExtent` uses different units.
                     * The margin and chunk size were applied during `baseExtent` computation and copied in `indices`.
                     */
                    scaledExtent = new GridExtent(indices, rounding, clipping, null, null, null, modifiedDimensions);
                    if (extent.equals(scaledExtent)) scaledExtent = extent;                 // Share common instance.
                    affine = Matrices.createIdentity(dimension + 1);
                    for (int k=0; k<subsampling.length; k++) {
                        final double s = subsampling[k];
                        if (s > 1) {                                                 // Also for skipping NaN values.
                            final int i = (modifiedDimensions != null) ? modifiedDimensions[k] : k;
                            affine.setElement(i, i, s);
                            affine.setElement(i, dimension, extent.getLow(i) - scaledExtent.getLow(i) * s);
                            // TODO: use Math.fma with JDK9.
                        }
                    }
                    toBase = MathTransforms.linear(affine);
                }
            }
        } catch (FactoryException | TransformException e) {
            throw new IllegalGridGeometryException(e, "areaOfInterest");
        }
        modifiedDimensions = null;                  // Not needed anymore.
        return this;
    }

    /**
     * Drops the source dimensions that are not needed for producing the target dimensions.
     * The retained source dimensions are stored in {@link #modifiedDimensions}.
     * This method is invoked in an effort to make the transform invertible.
     *
     * @param  cornerToCRS  transform from grid coordinates to AOI coordinates.
     * @param  dimension    value of {@code cornerToCRS.getTargetDimensions()}.
     */
    private MathTransform dropUnusedDimensions(MathTransform cornerToCRS, final int dimension)
            throws FactoryException, TransformException
    {
        if (dimension < cornerToCRS.getSourceDimensions()) {
            final TransformSeparator sep = new TransformSeparator(cornerToCRS);
            cornerToCRS = sep.separate();
            modifiedDimensions = sep.getSourceDimensions();
            if (modifiedDimensions.length != dimension) {
                throw new TransformException(Resources.format(Resources.Keys.CanNotMapToGridDimensions));
            }
        }
        return cornerToCRS;
    }

    /**
     * Returns the point of interest of current {@link #baseExtent}, keeping only the remaining
     * dimensions after {@link #dropUnusedDimensions(MathTransform, int)} execution.
     * The position is in units of {@link #base} grid coordinates.
     */
    private double[] getPointOfInterest() {
        final double[] pointOfInterest = baseExtent.getPointOfInterest();
        if (modifiedDimensions == null) {
            return pointOfInterest;
        }
        final double[] filtered = new double[modifiedDimensions.length];
        for (int i=0; i<filtered.length; i++) {
            filtered[i] = pointOfInterest[modifiedDimensions[i]];
        }
        return filtered;
    }

    /**
     * Sets {@link #baseExtent} to the given envelope clipped to the previous extent.
     * This method shall be invoked for clipping only, without any subsampling applied.
     * The context for invoking this method is:
     *
     * <ul>
     *   <li>{@link #subgrid(GridGeometry)} before subsampling is applied.</li>
     *   <li>{@link #subgrid(Envelope, double...)} before resolution is applied.</li>
     * </ul>
     *
     * As a consequence of above context, margin and chunk size are in units of the base extent.
     * They are not in units of cells of the size that we get after subsampling.
     *
     * @param  indices  the envelope to intersect in units of {@link #base} grid coordinates.
     * @throws DisjointExtentException if the given envelope does not intersect the grid extent.
     *
     * @see #getBaseExtentExpanded(boolean)
     */
    private void setBaseExtentClipped(final GeneralEnvelope indices) {
        final GridExtent sub = new GridExtent(indices, rounding, clipping, margin, chunkSize, baseExtent, modifiedDimensions);
        if (!sub.equals(baseExtent)) {
            baseExtent = sub;
        }
        isBaseExtentExpanded = true;
    }

    /**
     * Requests a grid geometry over a sub-region of the base grid geometry and optionally with subsampling.
     * The given grid geometry must have the same number of dimension than the base grid geometry.
     * If the length of {@code subsampling} array is less than the number of dimensions,
     * then no subsampling will be applied on the missing dimensions.
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>This method can be invoked only once.</li>
     *   <li>This method can not be used together with another {@code subgrid(…)} method.</li>
     *   <li>{@linkplain #rounding(GridRoundingMode) Rounding mode}, {@linkplain #clipping(GridClippingMode) clipping mode},
     *       {@linkplain #margin(int...) margin} and {@linkplain #chunkSize(int...) chunk size},
     *       if different than default values, should be set before to invoke this method.</li>
     *   <li>{@linkplain #slice(DirectPosition) Slicing} can be applied after this method.</li>
     *   <li>This method does not reduce the number of dimensions of the grid geometry.
     *       For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</li>
     * </ul>
     *
     * @param  areaOfInterest  the desired grid extent in unit of base grid cell (i.e. ignoring subsampling),
     *                         or {@code null} for not restricting the sub-grid to a sub-area.
     * @param  subsampling     the subsampling to apply on each grid dimension, or {@code null} if none.
     *                         All values shall be greater than zero. If the array length is shorter than
     *                         the number of dimensions, missing values are assumed to be 1.
     * @return {@code this} for method call chaining.
     * @throws DisjointExtentException if the given area of interest does not intersect the grid extent.
     * @throws IncompleteGridGeometryException if the base grid geometry has no extent, no "grid to CRS" transform,
     *         or no CRS (unless {@code areaOfInterest} has no CRS neither, in which case the CRS are assumed the same).
     * @throws IllegalStateException if a {@link #subgrid(GridGeometry) subgrid(…)} or {@link #slice(DirectPosition) slice(…)}
     *         method has already been invoked.
     *
     * @see #getIntersection()
     * @see #getSubsampling()
     *
     * @since 1.1
     */
    public GridDerivation subgrid(final GridExtent areaOfInterest, final int... subsampling) {
        ensureSubgridNotSet();
        final int n = base.getDimension();
        if (areaOfInterest != null) {
            final int actual = areaOfInterest.getDimension();
            if (actual != n) {
                throw new IllegalArgumentException(Errors.format(
                        Errors.Keys.MismatchedDimension_3, "extent", n, actual));
            }
        }
        subGridSetter = "subgrid";
        if (areaOfInterest != null && baseExtent != null) {
            baseExtent = baseExtent.intersect(areaOfInterest);
        }
        return (subsampling != null) ? subsample(subsampling) : this;
    }

    /**
     * Applies a subsampling on the grid geometry to build.
     * This method can be invoked as an alternative to {@code subgrid(…)} methods if only the resolution needs to be changed.
     * The {@linkplain GridGeometry#getExtent() extent} of the {@linkplain #build() built} grid geometry will be derived
     * from {@link #getIntersection()} as below for each dimension <var>i</var>:
     *
     * <ul>
     *   <li>The {@linkplain GridExtent#getLow(int)  low}  is divided by {@code subsampling[i]}, rounded toward zero.</li>
     *   <li>The {@linkplain GridExtent#getSize(int) size} is divided by {@code subsampling[i]}, rounded toward zero.</li>
     *   <li>The {@linkplain GridExtent#getHigh(int) high} is recomputed from above low and size.</li>
     * </ul>
     *
     * The {@linkplain GridGeometry#getGridToCRS(PixelInCell) grid to CRS} transform is scaled accordingly
     * in order to map approximately to the same {@linkplain GridGeometry#getEnvelope() envelope}.
     *
     * @param  subsampling  the subsampling to apply on each grid dimension. All values shall be greater than zero.
     *         If the array length is shorter than the number of dimensions, missing values are assumed to be 1.
     * @return {@code this} for method call chaining.
     * @throws IllegalStateException if a subsampling has already been set,
     *         for example by a call to {@link #subgrid(Envelope, double...) subgrid(…)}.
     *
     * @see #subgrid(GridExtent, int...)
     * @see #getSubsampling()
     * @see GridExtent#subsample(int...)
     *
     * @deprecated Replaced by {@link #subgrid(GridExtent, int...)} with a {@code null} extent.
     */
    @Deprecated
    // TODO: make private (do not delete) after next SIS release.
    // This method assumes that subsampling are divisors of chunk sizes.
    public GridDerivation subsample(final int... subsampling) {
        ArgumentChecks.ensureNonNull("subsampling", subsampling);
        if (toBase != null) {
            throw new IllegalStateException(Errors.format(Errors.Keys.ValueAlreadyDefined_1, "subsampling"));
        }
        if (subGridSetter == null) {
            subGridSetter = "subsample";
        }
        // Validity of the subsampling values will be verified by GridExtent.subsample(…) invoked below.
        final GridExtent extent = getBaseExtentExpanded(true);
        Matrix affine = null;
        final int dimension = extent.getDimension();
        for (int i = Math.min(dimension, subsampling.length); --i >= 0;) {
            final int s = subsampling[i];
            if (s != 1) {
                if (affine == null) {
                    affine = Matrices.createIdentity(dimension + 1);
                    scaledExtent = extent.subsample(subsampling);
                }
                final long offset = Math.subtractExact(extent.getLow(i), Math.multiplyExact(scaledExtent.getLow(i), s));
                affine.setElement(i, i, s);
                affine.setElement(i, dimension, offset);
            }
        }
        if (affine != null) {
            toBase = MathTransforms.linear(affine);
        }
        return this;
    }

    /**
     * Requests a grid geometry for a slice at the given "real world" position.
     * The given position can be expressed in any coordinate reference system (CRS).
     * The position should not define a coordinate for all dimensions, otherwise the slice would degenerate
     * to a single point. Dimensions can be left unspecified either by assigning to {@code slicePoint} a CRS
     * without those dimensions, or by assigning the NaN value to some coordinates.
     *
     * <div class="note"><b>Example:</b>
     * if the {@linkplain GridGeometry#getCoordinateReferenceSystem() coordinate reference system} of base grid geometry has
     * (<var>longitude</var>, <var>latitude</var>, <var>time</var>) axes, then a (<var>longitude</var>, <var>latitude</var>)
     * slice at time <var>t</var> can be created with one of the following two positions:
     * <ul>
     *   <li>A three-dimensional position with ({@link Double#NaN}, {@link Double#NaN}, <var>t</var>) coordinates.</li>
     *   <li>A one-dimensional position with (<var>t</var>) coordinate and the coordinate reference system set to
     *       {@linkplain org.apache.sis.referencing.CRS#getTemporalComponent(CoordinateReferenceSystem) the temporal component}
     *       of the grid geometry CRS.</li>
     * </ul></div>
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>This method can be invoked after {@link #subgrid(Envelope, double...)}, but not before.</li>
     *   <li>If a non-default rounding mode is desired, it should be {@linkplain #rounding(GridRoundingMode) specified}
     *       before to invoke this method.</li>
     *   <li>This method does not reduce the number of dimensions of the grid geometry.
     *       For dimensionality reduction, see {@link GridGeometry#reduce(int...)}.</li>
     *   <li>If the given point is known to be expressed in the same CRS than the grid geometry,
     *       then the {@linkplain DirectPosition#getCoordinateReferenceSystem() CRS of the point}
     *       can be left unspecified ({@code null}). It may give a slight performance improvement
     *       by avoiding the check for coordinate transformation.</li>
     * </ul>
     *
     * @param  slicePoint   the coordinates where to get a slice. If no coordinate reference system is attached to it,
     *                      we consider it's the same as base grid geometry.
     * @return {@code this} for method call chaining.
     * @throws IncompleteGridGeometryException if the base grid geometry has no extent, no "grid to CRS" transform,
     *         or no CRS (unless {@code slicePoint} has no CRS neither, in which case the CRS are assumed the same).
     * @throws IllegalGridGeometryException if an error occurred while converting the point coordinates to grid coordinates.
     * @throws PointOutsideCoverageException if the given point is outside the grid extent.
     */
    public GridDerivation slice(DirectPosition slicePoint) {
        ArgumentChecks.ensureNonNull("slicePoint", slicePoint);
        MathTransform gridToCRS = base.requireGridToCRS(true);
        subGridSetter = "slice";
        try {
            if (toBase != null) {
                gridToCRS = MathTransforms.concatenate(toBase, gridToCRS);
            }
            /*
             * We will try to find a path between grid coordinate reference system (CRS) and given point CRS. Note that we
             * allow unknown CRS on the slice point, in which case we consider it to be expressed in grid reference system.
             * However, if the point CRS is specified while the base grid CRS is unknown, we are at risk of ambiguity,
             * in which case we throw (indirectly) an IncompleteGridGeometryException.
             */
            final CoordinateReferenceSystem sliceCRS = slicePoint.getCoordinateReferenceSystem();
            final MathTransform baseToPOI;
            if (sliceCRS == null) {
                baseToPOI = null;
            } else {
                slicePoint = new DimensionReducer(base, sliceCRS).apply(slicePoint);
                final CoordinateReferenceSystem gridCRS = base.getCoordinateReferenceSystem();      // May throw exception.
                baseToPOI = CRS.findOperation(gridCRS, sliceCRS, null).getMathTransform();
                gridToCRS = MathTransforms.concatenate(gridToCRS, baseToPOI);
            }
            /*
             * If the point dimensions do not encompass all grid dimensions, the transform is probably non-invertible.
             * We need to reduce the number of grid dimensions in the transform for having a one-to-one relationship.
             */
            final int dimension = gridToCRS.getTargetDimensions();
            ArgumentChecks.ensureDimensionMatches("slicePoint", dimension, slicePoint);
            gridToCRS = dropUnusedDimensions(gridToCRS, dimension);
            /*
             * Take in account the case where the point could be inside the grid if we apply a ±360° longitude shift.
             * This is the same adjustment than for `subgrid(Envelope)`, but applied on a DirectPosition. Calculation
             * is done in units of cells of the GridGeometry to be created by GridDerivation.
             */
            DirectPosition gridPoint = new WraparoundAdjustment(base.envelope, baseToPOI, gridToCRS.inverse()).shift(slicePoint);
            if (scaledExtent != null) {
                scaledExtent = scaledExtent.slice(gridPoint, modifiedDimensions);
            }
            /*
             * Above `scaledExtent` is non-null only if a scale or subsampling has been applied before this `slice`
             * method has been invoked. The `baseExtent` below contains same information, but without subsampling.
             * The subsampling effect is removed by applying the "scaled to base" transform. Accuracy matter less
             * here than for `scaledExtent` since the extent to be returned to user is the later.
             */
            if (toBase != null) {
                gridPoint = toBase.transform(gridPoint, gridPoint);
            }
            // Non-null check was done by `base.requireGridToCRS()`.
            baseExtent = getBaseExtentExpanded(true).slice(gridPoint, modifiedDimensions);
        } catch (FactoryException e) {
            throw new IllegalGridGeometryException(Resources.format(Resources.Keys.CanNotMapToGridDimensions), e);
        } catch (TransformException e) {
            throw new IllegalGridGeometryException(e, "slicePoint");
        }
        modifiedDimensions = null;              // Not needed anymore.
        return this;
    }

    /**
     * Requests a grid geometry for a slice at the given relative position.
     * The relative position is specified by a ratio between 0 and 1 where 0 maps to {@linkplain GridExtent#getLow(int) low}
     * grid coordinates, 1 maps to {@linkplain GridExtent#getHigh(int) high grid coordinates} and 0.5 maps to the median position.
     * The slicing is applied on all dimensions except the specified dimensions to keep.
     *
     * <div class="note"><b>Example:</b>
     * given a <var>n</var>-dimensional cube, the following call creates a slice of the two first dimensions
     * (numbered 0 and 1, typically the dimensions of <var>x</var> and <var>y</var> axes)
     * located at the center (ratio 0.5) of all other dimensions (typically <var>z</var> and/or <var>t</var> axes):
     *
     * {@preformat java
     *     gridGeometry.derive().sliceByRatio(0.5, 0, 1).build();
     * }
     * </div>
     *
     * @param  sliceRatio        the ratio to apply on all grid dimensions except the ones to keep.
     * @param  dimensionsToKeep  the grid dimension to keep unchanged.
     * @return {@code this} for method call chaining.
     * @throws IncompleteGridGeometryException if the base grid geometry has no extent.
     * @throws IndexOutOfBoundsException if a {@code dimensionsToKeep} value is out of bounds.
     */
    public GridDerivation sliceByRatio(final double sliceRatio, final int... dimensionsToKeep) {
        ArgumentChecks.ensureBetween("sliceRatio", 0, 1, sliceRatio);
        ArgumentChecks.ensureNonNull("dimensionsToKeep", dimensionsToKeep);
        subGridSetter = "sliceByRatio";
        final GridExtent extent = getBaseExtentExpanded(true);
        final GeneralDirectPosition slicePoint = new GeneralDirectPosition(extent.getDimension());
        baseExtent = extent.sliceByRatio(slicePoint, sliceRatio, dimensionsToKeep);
        if (scaledExtent != null) {
            scaledExtent = scaledExtent.sliceByRatio(slicePoint, sliceRatio, dimensionsToKeep);
        }
        return this;
    }

    /*
     * RATIONAL FOR NOT PROVIDING reduce(int... dimensions) METHOD HERE: that method would need to be the last method invoked,
     * otherwise it makes more complicated to implement other methods in this class.  Forcing users to invoke `build()` before
     * (s)he can invoke GridGeometry.reduce(…) makes that clear and avoid the need for more flags in this GridDerivation class.
     * Furthermore declaring the `reduce(…)` method in GridGeometry is more consistent with `GridExtent.reduce(…)`.
     */

    /**
     * Builds a grid geometry with the configuration specified by the other methods in this {@code GridDerivation} class.
     *
     * @return the modified grid geometry. May be the {@link #base} grid geometry if no change apply.
     * @throws IllegalGridGeometryException if the grid geometry can not be computed
     *         because of arguments given to a {@code subgrid(…)} or other methods.
     *
     * @see #getIntersection()
     */
    public GridGeometry build() {
        /*
         * Assuming:
         *
         *   • All low coordinates = 0
         *   • h₁ the high coordinate before subsampling
         *   • h₂ the high coordinates after subsampling
         *   • c  a conversion factor from grid indices to "real world" coordinates
         *   • s  a subsampling ≥ 1
         *
         * Then the envelope upper bounds x is:
         *
         *   • x = (h₁ + 1) × c
         *   • x = (h₂ + f) × c⋅s      which implies       h₂ = h₁/s      and       f = 1/s
         *
         * If we modify the later equation for integer division instead than real numbers, we have:
         *
         *   • x = (h₂ + f) × c⋅s      where        h₂ = floor(h₁/s)      and       f = ((h₁ mod s) + 1)/s
         *
         * Because s ≥ 1, then f ≤ 1. But the f value actually used by GridExtent.toCRS(…) is hard-coded to 1
         * since it assumes that all cells are whole, i.e. it does not take in account that the last cell may
         * actually be fraction of a cell. Since 1 ≥ f, the computed envelope may be larger. This explains the
         * need for envelope clipping performed by GridGeometry constructor.
         */
        final GridExtent extent = (scaledExtent != null) ? scaledExtent : getBaseExtentExpanded(false);
        try {
            if (toBase != null || extent != base.extent) {
                return new GridGeometry(base, extent, toBase);
            }
            /*
             * Intersection should be non-null only if we have not been able to compute more reliable properties
             * (grid extent and "grid to CRS" transform). It should happen only if `gridToCRS` is null, but we
             * nevertheless pass that transform to the constructor as a matter of principle.
             */
            if (intersection != null) {
                return new GridGeometry(PixelInCell.CELL_CENTER, base.gridToCRS, intersection, rounding);
            }
            /*
             * Case when the only settings were a margin or a chunk size. It is okay to test after `intersection`
             * because a non-null envelope intersection would have meant that this `GridDerivation` does not have
             * required information for applying a margin anyway (no `GridExtent` and no `gridToCRS`).
             */
            final GridExtent resized = getBaseExtentExpanded(false);
            if (resized != baseExtent) {
                return new GridGeometry(base, resized, null);
            }
        } catch (TransformException e) {
            throw new IllegalGridGeometryException(e, "envelope");
        }
        return base;
    }

    /**
     * Returns {@link #baseExtent} with {@linkplain #margin} and {@linkplain #chunkSize chunk size} applied.
     *
     * @param  nonNull  whether the returned value should be guaranteed non-null.
     * @throws IncompleteGridGeometryException if {@code nonNull} is {@code true} and the grid geometry has no extent.
     *
     * @see #setBaseExtentClipped(GeneralEnvelope)
     */
    private GridExtent getBaseExtentExpanded(final boolean nonNull) {
        if (nonNull && baseExtent == null) {
            baseExtent = base.getExtent();          // Expected to throw IncompleteGridGeometryException.
        }
        if (!isBaseExtentExpanded) {
            if (baseExtent != null && (margin != null || chunkSize != null)) {
                GridExtent resized = baseExtent;
                if (margin != null) {
                    resized = resized.expand(ArraysExt.copyAsLongs(margin));
                }
                if (chunkSize != null) {
                    resized = resized.forChunkSize(chunkSize);
                }
                if (clipping == GridClippingMode.STRICT) {
                    resized = resized.intersect(base.extent);
                }
                if (!resized.equals(baseExtent)) {
                    baseExtent = resized;
                }
            }
            isBaseExtentExpanded = true;
        }
        return baseExtent;
    }

    /**
     * Returns the extent of the modified grid geometry, ignoring subsampling or changes in resolution.
     * This is the intersection of the {@link #base} grid geometry with the (grid or geospatial) envelope
     * given to a {@link #subgrid(Envelope, double...) subgrid(…)} method,
     * expanded by the {@linkplain #margin(int...) specified margin} (if any)
     * and potentially with some {@linkplain GridExtent#getSize(int) grid sizes} set to 1
     * if a {@link #slice(DirectPosition) slice(…)} method has been invoked.
     * The returned extent is in units of the {@link #base} grid cells, i.e.
     * {@linkplain #getSubsampling() subsampling} is ignored.
     *
     * <p>This method can be invoked after {@link #build()} for getting additional information.</p>
     *
     * @return intersection of grid geometry extents in units of {@link #base} grid cells.
     */
    public GridExtent getIntersection() {
        return getBaseExtentExpanded(true);
    }

    /**
     * @deprecated Renamed {@link #getSubsampling()} (without "s" because "subsampling" is uncountable).
     */
    @Deprecated
    public int[] getSubsamplings() {
        final int[] subsamplings;
        if (toBase == null) {
            subsamplings = new int[base.getDimension()];
            Arrays.fill(subsamplings, 1);
        } else {
            subsamplings = new int[toBase.getSourceDimensions()];
            final Matrix affine = toBase.getMatrix();
            for (int i=0; i < subsamplings.length; i++) {
                subsamplings[i] = roundSubsampling(affine.getElement(i,i), i);
            }
        }
        return subsamplings;
    }

    /**
     * Returns the strides for accessing cells along each axis of the base grid.
     * Those values define part of the conversion from <em>derived</em> grid coordinates
     * (<var>x</var>, <var>y</var>, <var>z</var>) to {@linkplain #base} grid coordinates
     * (<var>x′</var>, <var>y′</var>, <var>z′</var>) as below (generalize to as many dimensions as needed):
     *
     * <ul>
     *   <li><var>x′</var> = s₀⋅<var>x</var> + t₀</li>
     *   <li><var>y′</var> = s₁⋅<var>y</var> + t₁</li>
     *   <li><var>z′</var> = s₂⋅<var>z</var> + t₂</li>
     * </ul>
     *
     * This method returns the {s₀, s₁, s₂} values while {@link #getSubsamplingOffsets()}
     * returns the {t₀, t₁, t₂} values. All subsampling values are strictly positive integers.
     *
     * <div class="note"><b>Application to iterations</b><br>
     * Iteration over {@code areaOfInterest} grid coordinates with a stride Δ<var>x</var>=1
     * corresponds to an iteration in {@link #base} grid coordinates with a stride of Δ<var>x′</var>=s₀,
     * a stride Δ<var>y</var>=1 corresponds to a stride Δ<var>y′</var>=s₁, <i>etc.</i></div>
     *
     * This method can be invoked after {@link #build()} for getting additional information.
     * If {@link #subgrid(GridExtent, int...)} has been invoked, then this method returns the
     * values that were given in the {@code subsampling} argument.
     *
     * @return an <em>estimation</em> of the strides for accessing cells along each axis of {@link #base} grid.
     * @throws IllegalStateException if the subsampling factors are not integers. It may happen if the derived
     *         grid has been constructed by a call to {@link #subgrid(Envelope, double...)}.
     *
     * @see #getSubsamplingOffsets()
     * @see #subgrid(GridGeometry)
     * @see #subgrid(GridExtent, int...)
     *
     * @since 1.1
     */
    public int[] getSubsampling() {
        final int[] subsampling;
        if (toBase == null) {
            subsampling = new int[base.getDimension()];
            Arrays.fill(subsampling, 1);
        } else {
            subsampling = new int[toBase.getTargetDimensions()];
            final Matrix affine = toBase.getMatrix();
            for (int j=0; j < subsampling.length; j++) {
                final double e = affine.getElement(j,j);
                if ((subsampling[j] = (int) e) != e) {
                    throw new IllegalStateException(Errors.format(Errors.Keys.NotAnInteger_1, e));
                }
            }
        }
        return subsampling;
    }

    /**
     * Rounds a subsampling value according the current {@link RoundingMode}.
     * If a {@link #chunkSize} has been specified, then the subsampling will be a divisor of that size.
     * This is necessary for avoiding a drift of subsampled pixel coordinates computed from tile coordinates.
     *
     * <div class="note"><b>Drift example:</b>
     * if the tile size is 16 pixels and the subsampling is 3, then the subsampled tile size is ⌊16/3⌋ = 5 pixels.
     * Pixel coordinates for each tile is as below:
     *
     * <table class="sis">
     *   <caption>Tile and pixel coordinates for subsampling of 3 pixels</caption>
     *   <tr><th>Tile index</th> <th>Pixel coordinate</th> <th>Subsampled pixel coordinate</th></tr>
     *   <tr><td>0</td> <td> 0</td>  <td>0</td></tr>
     *   <tr><td>1</td> <td>16</td>  <td>5</td></tr>
     *   <tr><td>2</td> <td>32</td> <td>10</td></tr>
     *   <tr><td>3</td> <td>48</td> <td>16</td></tr>
     * </table>
     *
     * Note the last subsampled pixel coordinate: we have ⌊48/3⌋ = 16 pixels while 15 would have been expected
     * for a regular progression of those pixel coordinates. For {@code GridCoverageResource} implementations,
     * it would require to read the last row of tile #2 and insert those data as the first row of tile #3.
     * It does not only make implementations much more difficult, but also hurts performance because fetching
     * a single tile would actually require the "physical" reading of 2 or more tiles.</div>
     *
     * @param  scale      the scale factor to round.
     * @param  dimension  the dimension of the scale factor to round.
     */
    private int roundSubsampling(final double scale, final int dimension) {
        final int subsampling;
        switch (rounding) {
            default:        throw new AssertionError(rounding);
            case NEAREST:   subsampling = (int) Math.min(Math.round(scale), Integer.MAX_VALUE); break;
            case CONTAINED: subsampling = (int) Math.ceil(scale - tolerance(dimension)); break;
            case ENCLOSING: subsampling = (int) (scale + tolerance(dimension)); break;
        }
        if (subsampling <= 1) {
            return 1;
        }
        if (chunkSize != null) {
            final int size = chunkSize[dimension];
            final int r = subsampling % size;
            if (r > 1 && (size % r) != 0) {
                final int[] divisors = MathFunctions.divisors(size);
                final int i = ~Arrays.binarySearch(divisors, r);
                /*
                 * `binarySearch(…)` should never find an exact match, otherwise (size % r) would have been zero.
                 * Furthermore `i` should never be 0 because divisors[0] = 1, which can not be selected if r > 1.
                 * We do not check `if (i > 0)` "as a safety" because client code such as `TiledGridCoverage`
                 * will behave erratically if this method does not fulfill its contract (i.e. find a divisor).
                 * It is better to know now if there is any problem here.
                 */
                int s = divisors[i-1];
                if (i < divisors.length) {
                    switch (rounding) {
                        case CONTAINED: {
                            s = divisors[i];
                            break;
                        }
                        case NEAREST: {
                            final int above = divisors[i];
                            if (above - r < r - s) {
                                s = above;
                            }
                            break;
                        }
                    }
                }
                return s + (subsampling - r);
            }
        }
        return subsampling;
    }

    /**
     * Returns a tolerance factor for comparing scale factors in the given dimension.
     * The tolerance is such that the errors of pixel coordinates computed using the
     * scale factor should not be greater than 0.5 pixel.
     */
    private double tolerance(final int dimension) {
        return (base.extent != null) ? 0.5 / base.extent.getSize(dimension, false) : 0;
    }

    /**
     * Returns the offsets to be subtracted from pixel coordinates before subsampling.
     * In a conversion from <em>derived</em> grid to {@linkplain #base} grid coordinates
     * (the opposite direction of subsampling), the offset is the value to add after
     * multiplication by the scale factor. It may be negative.
     *
     * <p>This method can be invoked after {@link #build()} for getting additional information.</p>
     *
     * @return conversion from the new grid to the original grid specified to the constructor.
     * @throws IllegalStateException if the subsampling offsets are not integers. It may happen if the
     *         derived grid has been constructed by a call to {@link #subgrid(Envelope, double...)}.
     *
     * @see #getSubsampling()
     * @see #subgrid(GridGeometry)
     * @see #subgrid(GridExtent, int...)
     *
     * @since 1.1
     */
    public int[] getSubsamplingOffsets() {
        final int[] offsets;
        if (toBase == null) {
            offsets = new int[base.getDimension()];
        } else {
            final int srcDim = toBase.getSourceDimensions();
            offsets = new int[toBase.getTargetDimensions()];
            final Matrix affine = toBase.getMatrix();
            for (int j=0; j < offsets.length; j++) {
                final double e = affine.getElement(j, srcDim);
                if ((offsets[j] = (int) e) != e) {
                    throw new IllegalStateException(Errors.format(Errors.Keys.NotAnInteger_1, e));
                }
            }
        }
        return offsets;
    }

    /**
     * Returns an <em>estimation</em> of the scale factor when converting sub-grid coordinates to {@link #base} grid coordinates.
     * This is for information purpose only since this method combines potentially different scale factors for all dimensions.
     *
     * @return an <em>estimation</em> of the scale factor for all dimensions.
     *
     * @deprecated To be removed for avoiding operations that mix potentially unrelated dimensions.
     */
    @Deprecated
    public double getGlobalScale() {
        return java.util.stream.IntStream.of(getSubsampling()).average().getAsDouble();
    }

    /**
     * Returns a tree representation of this {@code GridDerivation}.
     * The tree representation is for debugging purpose only and may change in any future SIS version.
     *
     * @param  locale  the locale to use for textual labels.
     * @return a tree representation of this {@code GridDerivation}.
     */
    @Debug
    private TreeTable toTree(final Locale locale) {
        final TableColumn<CharSequence> column = TableColumn.VALUE_AS_TEXT;
        final TreeTable tree = new DefaultTreeTable(column);
        final TreeTable.Node root = tree.getRoot();
        root.setValue(column, Classes.getShortClassName(this));
        final StringBuilder buffer = new StringBuilder(256);
        /*
         * GridDerivation (example)
         *   └─Intersection
         *       ├─Dimension 0: [ 2000 … 5475] (3476 cells)
         *       └─Dimension 1: [-1000 … 7999] (9000 cells)
         */
        if (baseExtent != null) {
            TreeTable.Node section = root.newChild();
            section.setValue(column, "Intersection");
            try {
                baseExtent.appendTo(buffer, Vocabulary.getResources(locale));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            for (final CharSequence line : CharSequences.splitOnEOL(buffer)) {
                String text = line.toString().trim();
                if (!text.isEmpty()) {
                    section.newChild().setValue(column, text);
                }
            }
        }
        /*
         * GridDerivation (example)
         *   └─Subsampling
         *       ├─ × {50, 300}
         *       └─ + {0, 0}
         */
        if (toBase != null) {
            final TreeTable.Node section = root.newChild();
            section.setValue(column, "Subsampling");
            boolean offsets = false;
            do {
                buffer.setLength(0);
                buffer.append(offsets ? '+' : '×').append(" {");
                final int srcDim = toBase.getSourceDimensions();
                final int tgtDim = toBase.getTargetDimensions();
                final Matrix affine = toBase.getMatrix();
                for (int j=0; j<tgtDim; j++) {
                    if (j != 0) buffer.append(", ");
                    buffer.append(affine.getElement(j, offsets ? srcDim : j));
                    StringBuilders.trimFractionalPart(buffer);
                }
                section.newChild().setValue(column, buffer.append('}').toString());
            } while ((offsets = !offsets) == true);
        }
        return tree;
    }

    /**
     * Returns a string representation of this {@code GridDerivation} for debugging purpose.
     * The returned string is implementation dependent and may change in any future version.
     *
     * @return a string representation of this {@code GridDerivation} for debugging purpose.
     */
    @Override
    public String toString() {
        return toTree(null).toString();
    }
}
