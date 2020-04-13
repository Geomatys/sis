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

import java.util.List;
import java.util.Arrays;
import java.util.Optional;
import java.awt.Rectangle;
import java.awt.image.RenderedImage;
import org.opengis.util.FactoryException;
import org.opengis.coverage.CannotEvaluateException;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.TransformException;
import org.opengis.referencing.operation.MathTransform;
import org.apache.sis.geometry.Envelopes;
import org.apache.sis.image.ImageProcessor;
import org.apache.sis.coverage.SampleDimension;
import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.internal.util.DoubleDouble;
import org.apache.sis.internal.referencing.ExtendedPrecisionMatrix;
import org.apache.sis.metadata.iso.extent.DefaultGeographicBoundingBox;
import org.apache.sis.referencing.operation.transform.LinearTransform;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.referencing.operation.matrix.MatrixSIS;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.referencing.CRS;
import org.apache.sis.util.ComparisonMode;
import org.apache.sis.util.Utilities;


/**
 * A multi-dimensional grid coverage where each two-dimensional slice is the resampling
 * of data from another grid coverage. This class is used when the resampling can not be
 * stored in a {@link GridCoverage2D}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Johann Sorel (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class ResampledGridCoverage extends GridCoverage {
    /**
     * The {@value} constant for identifying code specific to the two-dimensional case.
     */
    private static final int BIDIMENSIONAL = 2;

    /**
     * The coverage to resample.
     */
    final GridCoverage source;

    /**
     * The transform from cell coordinates in this coverage to cell coordinates in {@linkplain #source} coverage.
     * Note that an offset may exist between cell coordinates and pixel coordinates, so some translations may need
     * to be concatenated with this transform on an image-by-image basis.
     */
    private final MathTransform toSourceCorner, toSourceCenter;

    /**
     * The image processor to use for resampling operations. Its configuration shall not
     * be modified because this processor may be shared by different grid coverages.
     */
    private final ImageProcessor imageProcessor;

    /**
     * Indices of extent dimensions corresponding to image <var>x</var> and <var>y</var> coordinates.
     * Typical values are 0 for {@code xDimension} and 1 for {@code yDimension}, but different values
     * are allowed. This class select the dimensions having largest size.
     */
    private final int xDimension, yDimension;

    /**
     * Creates a new grid coverage which will be the resampling of the given source.
     *
     * @param  source          the coverage to resample.
     * @param  domain          the grid extent, CRS and conversion from cell indices to CRS.
     * @param  toSourceCorner  transform from cell corner coordinates in this coverage to source coverage.
     * @param  toSourceCenter  transform from cell center coordinates in this coverage to source coverage.
     * @param  processor       the image processor to use for resampling images.
     */
    private ResampledGridCoverage(final GridCoverage source, final GridGeometry domain,
                                  final MathTransform toSourceCorner,
                                  final MathTransform toSourceCenter,
                                  ImageProcessor processor)
    {
        super(source, domain);
        this.source         = source;
        this.toSourceCorner = toSourceCorner;
        this.toSourceCenter = toSourceCenter;
        final GridExtent extent = domain.getExtent();
        long size1 = 0; int idx1 = 0;
        long size2 = 0; int idx2 = 1;
        final int dimension = extent.getDimension();
        for (int i=0; i<dimension; i++) {
            final long size = extent.getSize(i);
            if (size > size1) {
                size2 = size1; idx2 = idx1;
                size1 = size;  idx1 = i;
            } else if (size > size2) {
                size2 = size;  idx2 = i;
            }
        }
        if (idx1 < idx2) {          // Keep (x,y) dimensions in the order they appear.
            xDimension = idx1;
            yDimension = idx2;
        } else {
            xDimension = idx2;
            yDimension = idx1;
        }
        /*
         * Get fill values from background values declared for each band, if any.
         * If no background value is declared, default is 0 for integer data or
         * NaN for floating point values.
         */
        final List<SampleDimension> bands = getSampleDimensions();
        final Number[] fillValues = new Number[bands.size()];
        for (int i=fillValues.length; --i >= 0;) {
            final SampleDimension band = bands.get(i);
            final Optional<Number> bg = band.getBackground();
            if (bg.isPresent()) {
                fillValues[i] = bg.get();
            }
        }
        processor = processor.clone();
        processor.setFillValues(fillValues);
        imageProcessor = GridCoverageProcessor.unique(processor);
    }

    /**
     * If this coverage can be represented as a {@link GridCoverage2D} instance,
     * returns such instance. Otherwise returns {@code this}.
     *
     * @param  isGeometryExplicit  whether grid extent or "grid to CRS" transform have been explicitly
     *         specified by user. In such case, this method will not be allowed to change those values.
     */
    private GridCoverage specialize(final boolean isGeometryExplicit) throws TransformException {
        GridExtent extent = gridGeometry.getExtent();
        if (extent.getDimension() < GridCoverage2D.MIN_DIMENSION || extent.getSubDimension() > BIDIMENSIONAL) {
            return this;
        }
        /*
         * If the transform is linear and the user did not specified explicitly a desired transform or grid extent
         * (i.e. user specified only a target CRS), keep same image with a different `gridToCRS` transform instead
         * than doing a resampling. The intent is to avoid creating a new image if user apparently doesn't care.
         */
        if (!isGeometryExplicit && toSourceCorner instanceof LinearTransform) {
            MathTransform gridToCRS = gridGeometry.getGridToCRS(PixelInCell.CELL_CORNER);
            if (gridToCRS instanceof LinearTransform) {
                final GridGeometry sourceGG = source.getGridGeometry();
                extent = sourceGG.getExtent();
                gridToCRS = MathTransforms.concatenate(toSourceCorner.inverse(), gridToCRS);
                final GridGeometry targetGG = new GridGeometry(extent, PixelInCell.CELL_CORNER, gridToCRS,
                                                               getCoordinateReferenceSystem());
                if (sourceGG.equals(targetGG, ComparisonMode.APPROXIMATE)) {
                    return source;
                }
                return new GridCoverage2D(source, targetGG, extent, source.render(null), xDimension, yDimension);
            }
        }
        return new GridCoverage2D(source, gridGeometry, extent, render(null), xDimension, yDimension);
    }

    /**
     * Checks if two grid geometries are equal, ignoring unspecified properties. If a geometry
     * has no extent or no {@code gridToCRS} transform, the missing property is not compared.
     * Same applies for the grid extent.
     *
     * @return {@code true} if the two geometries are equal, ignoring unspecified properties.
     */
    static boolean equivalent(final GridGeometry sourceGG, final GridGeometry targetGG) {
        return (!isDefined(sourceGG, targetGG, GridGeometry.EXTENT)
                || Utilities.equalsIgnoreMetadata(sourceGG.getExtent(),
                                                  targetGG.getExtent()))
            && (!isDefined(sourceGG, targetGG, GridGeometry.CRS)
                || Utilities.equalsIgnoreMetadata(sourceGG.getCoordinateReferenceSystem(),
                                                  targetGG.getCoordinateReferenceSystem()))
            && (!isDefined(sourceGG, targetGG, GridGeometry.GRID_TO_CRS)
                || Utilities.equalsIgnoreMetadata(sourceGG.getGridToCRS(PixelInCell.CELL_CORNER),
                                                  targetGG.getGridToCRS(PixelInCell.CELL_CORNER))
                || Utilities.equalsIgnoreMetadata(sourceGG.getGridToCRS(PixelInCell.CELL_CENTER),   // Its okay if only one is equal.
                                                  targetGG.getGridToCRS(PixelInCell.CELL_CENTER)))
            && (!isDefined(sourceGG, targetGG, GridGeometry.ENVELOPE)
              || isDefined(sourceGG, targetGG, GridGeometry.EXTENT | GridGeometry.GRID_TO_CRS)      // Compare only if not inferred.
              || sourceGG.equalsApproximately(targetGG.envelope));
    }

    /**
     * Returns whether the given property is defined in both grid geometries.
     *
     * @param  property  one of {@link GridGeometry} constants.
     */
    private static boolean isDefined(final GridGeometry sourceGG, final GridGeometry targetGG, final int property) {
        return targetGG.isDefined(property) && sourceGG.isDefined(property);
    }

    /**
     * Implementation of {@link GridCoverageProcessor#resample(GridCoverage, GridGeometry)}.
     * This method computes the <em>inverse</em> of the transform from <var>Source Grid</var>
     * to <var>Target Grid</var>. That transform will be computed using the following path:
     *
     * <blockquote>Target Grid  ⟶  Target CRS  ⟶  Source CRS  ⟶  Source Grid</blockquote>
     *
     * If the target {@link GridGeometry} is incomplete, this method provides default
     * values for the missing properties. The following cases may occur:
     *
     * <ul class="verbose">
     *   <li>
     *     User provided no {@link GridExtent}. This method will construct a "grid to CRS" transform
     *     preserving (at least approximately) axis directions and resolutions at the point of interest.
     *     Then a grid extent will be created with a size large enough for containing the original grid
     *     transformed by above <var>Source Grid</var> → <var>Target Grid</var> transform.
     *   </li><li>
     *     User provided only a {@link GridExtent}. This method will compute an envelope large enough
     *     for containing the projected coordinates, then a "grid to CRS" transform will be derived
     *     from the grid and the georeferenced envelope with an attempt to preserve axis directions
     *     at least approximately.
     *   </li><li>
     *     User provided only a "grid to CRS" transform. This method will transform the projected envelope
     *     to "grid units" using the specified transform and create a grid extent large enough to hold the
     *     result.</li>
     * </ul>
     *
     * @param  source  the grid coverage to resample.
     * @param  target  the desired geometry of returned grid coverage. May be incomplete.
     * @return a grid coverage with the characteristics specified in the given grid geometry.
     * @throws IncompleteGridGeometryException if the source grid geometry is missing an information.
     * @throws TransformException if some coordinates can not be transformed to the specified target.
     */
    static GridCoverage create(final GridCoverage source, final GridGeometry target, final ImageProcessor processor)
            throws FactoryException, TransformException
    {
        final CoordinateReferenceSystem sourceCRS = source.getCoordinateReferenceSystem();
        final CoordinateReferenceSystem targetCRS = target.isDefined(GridGeometry.CRS) ?
                                                    target.getCoordinateReferenceSystem() : sourceCRS;
        /*
         * Get the coordinate operation from source CRS to target CRS. It may be the identity operation,
         * or null only if there is not enough information for determining the operation. We try to take
         * envelopes in account because the operation choice may depend on the geographic area.
         */
        CoordinateOperation changeOfCRS = null;
        final GridGeometry sourceGG = source.getGridGeometry();
        if (sourceGG.isDefined(GridGeometry.ENVELOPE) && target.isDefined(GridGeometry.ENVELOPE)) {
            changeOfCRS = Envelopes.findOperation(sourceGG.getEnvelope(), target.getEnvelope());
        }
        if (changeOfCRS == null && sourceCRS != null && targetCRS != null) try {
            DefaultGeographicBoundingBox areaOfInterest = null;
            if (sourceGG.isDefined(GridGeometry.ENVELOPE)) {
                areaOfInterest = new DefaultGeographicBoundingBox();
                areaOfInterest.setBounds(sourceGG.getEnvelope());
            }
            changeOfCRS = CRS.findOperation(sourceCRS, targetCRS, areaOfInterest);
        } catch (IncompleteGridGeometryException e) {
            // Happen if the source GridCoverage does not define a CRS.
            GridCoverageProcessor.recoverableException("resample", e);
        }
        /*
         * Compute the transform from source pixels to target CRS (to be completed to target pixels later).
         * The following line may throw IncompleteGridGeometryException, which is desired because if that
         * transform is missing, we can not continue (we have no way to guess it).
         */
        MathTransform sourceCornerToCRS = sourceGG.getGridToCRS(PixelInCell.CELL_CORNER);
        MathTransform sourceCenterToCRS = sourceGG.getGridToCRS(PixelInCell.CELL_CENTER);
        if (changeOfCRS != null) {
            final MathTransform tr = changeOfCRS.getMathTransform();
            sourceCornerToCRS = MathTransforms.concatenate(sourceCornerToCRS, tr);
            sourceCenterToCRS = MathTransforms.concatenate(sourceCenterToCRS, tr);
        }
        /*
         * Compute the transform from target grid to target CRS. This transform may be unspecified,
         * in which case we need to compute a default transform trying to preserve resolution at the
         * point of interest.
         */
        boolean isGeometryExplicit = target.isDefined(GridGeometry.EXTENT);
        GridExtent targetExtent = isGeometryExplicit ? target.getExtent() : null;
        final MathTransform targetCenterToCRS;
        if (target.isDefined(GridGeometry.GRID_TO_CRS)) {
            isGeometryExplicit = true;
            targetCenterToCRS = target.getGridToCRS(PixelInCell.CELL_CENTER);
            if (targetExtent == null) {
                targetExtent = targetExtent(sourceGG.getExtent(), sourceCornerToCRS,
                        target.getGridToCRS(PixelInCell.CELL_CORNER).inverse(), false);
            }
        } else {
            /*
             * We will try to preserve resolution at the point of interest, which is typically in the center.
             * We will also try to align the grids in such a way that integer coordinates close to the point
             * of interest are integers in both grids. This correction is given by the `translation` vector.
             */
            final GridExtent sourceExtent = sourceGG.getExtent();
            final double[]   sourcePOI    = sourceExtent.getPointOfInterest();
            final double[]   targetPOI    = new double[sourceCenterToCRS.getTargetDimensions()];
            final MatrixSIS  vectors      = MatrixSIS.castOrCopy(MathTransforms.derivativeAndTransform(
                                                        sourceCenterToCRS, sourcePOI, 0, targetPOI, 0));
            final double[]   originToPOI  = vectors.multiply(sourcePOI);
            /*
             * The first column above gives the displacement in target CRS when moving is the source grid by one cell
             * toward right, and the second column gives the displacement when moving one cell toward up (positive y).
             * More columns may exist in 3D, 4D, etc. cases. We retain only the magnitudes of those vectors, in order
             * to build new vectors with directions parallel with target grid axes. There is one magnitude value for
             * each target CRS dimension. If there is more target grid dimensions than the amount of magnitude values
             * (unusual, but not forbidden), some grid dimensions will be ignored provided that their size is 1
             * (otherwise a SubspaceNotSpecifiedException is thrown).
             */
            final MatrixSIS magnitudes = vectors.normalizeColumns();          // Length is dimension  of source grid.
            final int       crsDim     = vectors.getNumRow();                 // Number of dimensions of target CRS.
            final int       gridDim    = target.getDimension();               // Number of dimensions of target grid.
            final int       mappedDim  = Math.min(magnitudes.getNumCol(), Math.min(crsDim, gridDim));
            final MatrixSIS crsToGrid  = Matrices.create(gridDim + 1, crsDim + 1, ExtendedPrecisionMatrix.ZERO);
            final int[]     dimSelect  = (gridDim > crsDim && targetExtent != null) ?
                                         targetExtent.getSubspaceDimensions(crsDim) : null;
            /*
             * The goal below is to build a target "gridToCRS" which perform the same axis swapping and flipping than
             * the source "gridToCRS". For example if the source "gridToCRS" was flipping y axis, then we want target
             * "gridToCRS" to also flips that axis, unless the transformation from "source CRS" to "target CRS" flips
             * that axis, in which case the result in target "gridToCRS" would be to not flip again:
             *
             *     (source gridToCRS)    →    (source CRS to target CRS)    →    (target gridToCRS)⁻¹
             *         flip y axis             no axis direction change              flip y axis
             *   or    flip y axis                  flip y axis                   no additional flip
             *
             * For each column, the row index of the greatest absolute value is taken as the target dimension where
             * to set vector magnitude in target "crsToGrid" matrix. That way, if transformation from source CRS to
             * target CRS does not flip or swap axis, target `gridToCRS` matrix should looks like source `gridToCRS`
             * matrix (i.e. zero and non-zero coefficients at the same places). If two vectors have their greatest
             * value on the same row, the largest value win (since the `vectors` matrix has been normalized to unit
             * vectors, values in different columns are comparable).
             */
            for (;;) {
                double max = 0;
                int tgDim = -1;                         // Grid dimension of maximal value.
                int tcDim = -1;                         // CRS dimension of maximal value.
                for (int i=0; i<mappedDim; i++) {
                    // `ci` differs from `i` only if the source grid has "too much" dimensions.
                    final int ci = (dimSelect != null) ? dimSelect[i] : i;
                    for (int j=0; j<crsDim; j++) {
                        final double m = Math.abs(vectors.getElement(j,ci));
                        if (m > max) {
                            max   = m;
                            tcDim = j;
                            tgDim = ci;
                        }
                    }
                }
                if (tgDim < 0) break;                   // No more non-zero value in the `vectors` matrix.
                for (int j=0; j<crsDim; j++) {
                    vectors.setElement(j, tgDim, 0);    // For preventing this column to be selected again.
                }
                final DoubleDouble m = DoubleDouble.castOrCopy(magnitudes.getNumber(0, tgDim));
                m.inverseDivide(1);
                crsToGrid.setNumber(tgDim, tcDim, m);   // Scale factor from CRS coordinates to grid coordinates.
                /*
                 * Move the point of interest in a place where conversion to source grid coordinates
                 * will be close to integer. The exact location does not matter; an additional shift
                 * will be applied later for translating to target grid extent.
                 */
                m.multiply(originToPOI[tcDim] - targetPOI[tcDim]);
                crsToGrid.setNumber(tgDim, crsDim, m);
            }
            crsToGrid.setElement(gridDim, crsDim, 1);
            /*
             * At this point we got a first estimation of "target CRS to grid" transform, without translation terms.
             * Apply the complete transform chain on source extent; this will give us a tentative target extent.
             * This tentative extent will be compared with desired target.
             */
            final GridExtent tentative = targetExtent(sourceExtent, sourceCornerToCRS,
                                                      MathTransforms.linear(crsToGrid), true);
            if (targetExtent == null) {
                // Create an extent of same size but with lower coordinates set to 0.
                if (tentative.startsAtZero()) {
                    targetExtent = tentative;
                } else {
                    final long[] coordinates = new long[gridDim * 2];
                    for (int i=0; i<gridDim; i++) {
                        coordinates[i + gridDim] = tentative.getSize(i) - 1;
                    }
                    targetExtent = new GridExtent(tentative, coordinates);
                }
            }
            /*
             * At this point we have the desired target extent and the extent that we actually got by applying
             * full "source to target" transform. Compute the scale and offset differences between target and
             * actual extents, then adjust matrix coefficients for compensating those differences.
             */
            final DoubleDouble scale  = new DoubleDouble();
            final DoubleDouble offset = new DoubleDouble();
            final DoubleDouble tmp    = new DoubleDouble();
            for (int j=0; j<gridDim; j++) {
                tmp.set(targetExtent.getSize(j));
                scale.set(tentative.getSize(j));
                scale.inverseDivide(tmp);

                tmp.set(targetExtent.getLow(j));
                offset.set(-tentative.getLow(j));
                offset.multiply(scale);
                offset.add(tmp);
                crsToGrid.convertAfter(j, scale, offset);
            }
            targetCenterToCRS = MathTransforms.linear(crsToGrid.inverse());
        }
        /*
         * At this point all target grid geometry components are non-null.
         * Build the final target GridGeometry if any components were missing.
         * If an envelope is defined, resample only that sub-region.
         */
        GridGeometry resampled = target;
        ComparisonMode mode = ComparisonMode.IGNORE_METADATA;
        if (!target.isDefined(GridGeometry.EXTENT | GridGeometry.GRID_TO_CRS | GridGeometry.CRS)) {
            resampled = new GridGeometry(targetExtent, PixelInCell.CELL_CENTER, targetCenterToCRS, targetCRS);
            mode = ComparisonMode.APPROXIMATE;
            if (target.isDefined(GridGeometry.ENVELOPE)) {
                final MathTransform targetCornerToCRS = resampled.getGridToCRS(PixelInCell.CELL_CORNER);
                GeneralEnvelope bounds = new GeneralEnvelope(resampled.getEnvelope());
                bounds.intersect(target.getEnvelope());
                bounds = Envelopes.transform(targetCornerToCRS.inverse(), bounds);
                targetExtent = new GridExtent(bounds, GridRoundingMode.ENCLOSING, null, targetExtent, null);
                resampled = new GridGeometry(targetExtent, PixelInCell.CELL_CENTER, targetCenterToCRS, targetCRS);
                isGeometryExplicit = true;
            }
        }
        if (sourceGG.equals(resampled, mode)) {
            return source;
        }
        /*
         * Complete the "target to source" transform.
         */
        final MathTransform targetCornerToCRS = resampled.getGridToCRS(PixelInCell.CELL_CORNER);
        return new ResampledGridCoverage(source, resampled,
                MathTransforms.concatenate(targetCornerToCRS, sourceCornerToCRS.inverse()),
                MathTransforms.concatenate(targetCenterToCRS, sourceCenterToCRS.inverse()),
                processor).specialize(isGeometryExplicit);
    }

    /**
     * Computes a target grid extent by transforming the source grid extent.
     *
     * @param  source       the source grid extent to transform.
     * @param  cornerToCRS  transform from source grid corners to target CRS.
     * @param  crsToGrid    transform from target CRS to target grid corners or centers.
     * @param  center       whether {@code crsToGrid} maps cell centers ({@code true}) or cell corners ({@code false}).
     * @return target grid extent.
     */
    private static GridExtent targetExtent(final GridExtent source, final MathTransform cornerToCRS,
            final MathTransform crsToGrid, final boolean center) throws TransformException
    {
        final MathTransform tr = MathTransforms.concatenate(cornerToCRS, crsToGrid);
        final GeneralEnvelope bounds = source.toCRS(tr, tr, null);
        if (center) {
            final double[] vector = new double[bounds.getDimension()];
            Arrays.fill(vector, 0.5);
            bounds.translate(vector);       // Convert cell centers to cell corners.
        }
        return new GridExtent(bounds, GridRoundingMode.NEAREST, null, null, null);
    }

    /**
     * Returns a two-dimensional slice of resampled grid data as a rendered image.
     */
    @Override
    public RenderedImage render(GridExtent sliceExtent) {
        if (sliceExtent != null) try {
            final GeneralEnvelope bounds = sliceExtent.toCRS(toSourceCorner, toSourceCenter, null);
            sliceExtent = new GridExtent(bounds, GridRoundingMode.ENCLOSING, null, null, null);
        } catch (TransformException e) {
            throw new CannotEvaluateException(e.getLocalizedMessage(), e);
        }
        final RenderedImage image        = source.render(sliceExtent);
        final GridExtent    sourceExtent = source.getGridGeometry().getExtent();
        final GridExtent    targetExtent = gridGeometry.getExtent();
        final Rectangle     bounds       = new Rectangle(Math.toIntExact(targetExtent.getSize(xDimension)),
                                                         Math.toIntExact(targetExtent.getSize(yDimension)));
        /*
         * `this.toSource` is a transform from source cell coordinates to target cell coordinates.
         * We need a transform from source pixel coordinates to target pixel coordinates (in images).
         * An offset may exist between cell coordinates and pixel coordinates.
         */
        final MathTransform pixelsToTransform = MathTransforms.translation(
                targetExtent.getLow(xDimension),
                targetExtent.getLow(yDimension));

        final MathTransform transformToPixels = MathTransforms.translation(
                Math.subtractExact(image.getMinX(), sourceExtent.getLow(xDimension)),
                Math.subtractExact(image.getMinY(), sourceExtent.getLow(yDimension)));

        final MathTransform toImage = MathTransforms.concatenate(pixelsToTransform, toSourceCenter, transformToPixels);
        return imageProcessor.resample(bounds, toImage, image);
    }
}