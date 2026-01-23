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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.sis.storage.GridCoverageResource;
import org.opengis.referencing.operation.Matrix;
import org.opengis.util.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.apache.sis.coverage.grid.GridCoverage;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.PixelInCell;
import org.apache.sis.geometry.GeneralDirectPosition;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.operation.matrix.MatrixSIS;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStoreReferencingException;
import org.apache.sis.storage.base.GridResourceWrapper;
import org.apache.sis.storage.base.StoreResource;
import org.apache.sis.util.Utilities;

/**
 * A resource that aggregates multiple grid coverage resources representing the
 * same data at different resolutions.
 *
 * @author Quentin Bialota (Geomatys)
 */
public final class MultiResolutionResource extends GridResourceWrapper implements StoreResource {
    /**
     * The resources at different resolutions, ordered from finest to coarsest.
     */
    private final GridCoverageResource[] levels;

    /**
     * Resolutions (in units of CRS axes) of each level from finest to coarsest
     * resolution.
     * Array elements may be {@code null} if not yet computed.
     */
    private final double[][] resolutions;

    /**
     * Creates a new multi-resolution resource.
     *
     * @param levels the resources at different resolutions, ordered from finest to
     *               coarsest.
     */
    public MultiResolutionResource(List<GridCoverageResource> levels) {
        this.levels = levels.toArray(GridCoverageResource[]::new);
        this.resolutions = new double[this.levels.length][];
    }

    /**
     * Returns the data store that produced this resource.
     */
    @Override
    public DataStore getOriginator() {
        if (levels[0] instanceof StoreResource) {
            return ((StoreResource) levels[0]).getOriginator();
        }
        return null;
    }

    /**
     * Gets the paths to files used by this resource, or an empty value if unknown.
     */
    @Override
    public final Optional<FileSet> getFileSet() throws DataStoreException {
        return levels[0].getFileSet();
    }

    /**
     * Returns the object on which to perform all synchronizations for
     * thread-safety.
     */
    @Override
    protected final Object getSynchronizationLock() {
        if (levels[0] instanceof StoreResource) {
            return ((StoreResource) levels[0]).getOriginator();
        }
        return null;
    }

    /**
     * Creates the resource to which to delegate operations.
     * The source is the first image, the one having finest resolution.
     */
    @Override
    protected GridCoverageResource createSource() throws DataStoreException {
        return levels[0];
    }

    /**
     * Returns the resolution (in units of CRS axes) for the given level.
     *
     * @param level the desired resolution level, numbered from finest to coarsest
     *              resolution.
     * @return resolution at the specified level, not cloned (caller shall not
     *         modify).
     */
    private double[] resolution(final int level) throws DataStoreException {
        double[] resolution = resolutions[level];
        if (resolution == null) {
            GridGeometry geometry = levels[level].getGridGeometry();
            if (geometry.isDefined(GridGeometry.RESOLUTION)) {
                resolution = geometry.getResolution(true);
            } else if (geometry.isDefined(GridGeometry.GRID_TO_CRS) && geometry.isDefined(GridGeometry.EXTENT)) {
                try {
                    Matrix derivative = geometry.getGridToCRS(PixelInCell.CELL_CENTER)
                            .derivative(new GeneralDirectPosition(
                                    geometry.getExtent().getPointOfInterest(PixelInCell.CELL_CENTER)));

                    resolution = MatrixSIS.castOrCopy(derivative).multiply(new double[derivative.getNumCol()]);
                } catch (TransformException e) {
                    throw new DataStoreReferencingException(e);
                }
            }

            if (resolution == null && level > 0) {
                resolution = deriveResolutionFromRelativeSize(level);
            }

            resolutions[level] = resolution;
        }
        return resolution;
    }

    /**
     * Estimates resolution by comparing the grid size of the given level to the
     * base level.
     */
    private double[] deriveResolutionFromRelativeSize(int level) throws DataStoreException {
        final GridGeometry baseGeom = levels[0].getGridGeometry();
        final GridGeometry levelGeom = levels[level].getGridGeometry();

        if (baseGeom.isDefined(GridGeometry.EXTENT) && levelGeom.isDefined(GridGeometry.EXTENT)) {
            double[] baseResolution = resolution(0);
            if (baseResolution == null)
                return null;

            double[] derived = new double[baseResolution.length];
            for (int i = 0; i < derived.length; i++) {
                long baseSpan = baseGeom.getExtent().getSize(i);
                long levelSpan = levelGeom.getExtent().getSize(i);
                double scale = (double) baseSpan / (double) levelSpan;
                derived[i] = baseResolution[i] * scale;
            }
            return derived;
        }
        return null;
    }

    /**
     * Returns the preferred resolutions (in units of CRS axes) for read operations
     * in this data store.
     * Elements are ordered from finest (smallest numbers) to coarsest (largest
     * numbers) resolution.
     */
    @Override
    public List<double[]> getResolutions() throws DataStoreException {
        double[][] copy = new double[resolutions.length][];
        for (int i = 0; i < copy.length; i++) {
            double[] r = resolution(i);
            copy[i] = (r != null) ? r.clone() : null;
        }
        return Arrays.asList(copy);
    }

    /**
     * Converts a resolution from units in the given CRS to units of this coverage
     * CRS.
     */
    private double[] getResolution(final GridGeometry domain) throws DataStoreException {
        if (domain == null || !domain.isDefined(GridGeometry.RESOLUTION)) {
            return null;
        }
        double[] resolution = domain.getResolution(true);
        if (domain.isDefined(GridGeometry.CRS))
            try {
                final CoordinateReferenceSystem crs = domain.getCoordinateReferenceSystem();
                final GridGeometry gg = getGridGeometry();
                final CoordinateReferenceSystem myCrs = gg.getCoordinateReferenceSystem();

                if (!Utilities.equalsIgnoreMetadata(crs, myCrs)) {

                    // Simplified transform logic compared to MultiResolutionImage for brevity,
                    // but ideally should be robust.
                    MathTransform op = CRS.findOperation(crs, myCrs, null).getMathTransform();
                    if (!op.isIdentity()) {
                        MatrixSIS derivative = MatrixSIS.castOrCopy(op.derivative(new GeneralDirectPosition(
                                domain.getExtent().getPointOfInterest(PixelInCell.CELL_CENTER))));
                        resolution = derivative.multiply(resolution);
                        for (int i = 0; i < resolution.length; i++) {
                            resolution[i] = Math.abs(resolution[i]);
                        }
                    }
                }
            } catch (FactoryException | TransformException e) {
                throw new DataStoreReferencingException(e);
            }
        return resolution;
    }

    @Override
    public GridCoverage read(final GridGeometry domain, final int... ranges) throws DataStoreException {
        final double[] request = getResolution(domain);
        int level = (request != null) ? levels.length : 1;

        finer: while (--level > 0) {
            final double[] resolution = resolution(level);
            if (resolution == null)
                continue;

            for (int i = 0; i < Math.min(request.length, resolution.length); i++) {
                if (!(request[i] >= resolution[i])) { // Use ! for catching NaN
                    continue finer;
                }
            }
            break;
        }

        return levels[level].read(domain, ranges);
    }
}
