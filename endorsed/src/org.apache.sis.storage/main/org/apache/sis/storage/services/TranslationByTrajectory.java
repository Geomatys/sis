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
package org.apache.sis.storage.services;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import org.opengis.feature.Feature;
import org.opengis.feature.Attribute;
import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.operation.Matrix;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.referencing.operation.NoninvertibleTransformException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.CommonCRS;
import org.apache.sis.referencing.crs.DefaultTemporalCRS;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.referencing.operation.transform.AbstractMathTransform;
import org.apache.sis.parameter.Parameters;
import org.apache.sis.util.ComparisonMode;
import org.apache.sis.util.Utilities;
import org.apache.sis.measure.Units;

// Temporary dependency for TestBed-19 (TODO: need to leave choice to user).
import org.locationtech.jts.geom.LineString;


/**
 * Datum shift from one CRS to another with a time-varying translation, where both CRS have axes oriented
 * in the same directions. The target CRS is attached to a feature in motion relatively to the source CRS.
 * The motion is described by a trajectory in a moving feature file. Translations are given by positions
 * interpolated at <var>t</var> coordinate values in the trajectory.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
final class TranslationByTrajectory extends AbstractMathTransform {
    /**
     * The URI specified in parameter, usually relative to an unspecified directory.
     * Used for the value to return in {@link #getParameterValues()}.
     */
    private final URI sourceFile;

    /**
     * The reference system of coordinates stored in the {@link #trajectory} array.
     */
    private final CoordinateReferenceSystem interpolationCRS;

    /**
     * Coordinate reference system of temporal coordinates stored in {@link #startTimes}.
     * It may be any temporal CRS, it does not need to be the same CRS than the one used
     * in the trajectory file.
     */
    private final DefaultTemporalCRS timeCRS;

    /**
     * Number of dimensions of the trajectory CRS.
     */
    private final int trajectoryDimension;

    /**
     * Values extracted from parameters declared in the moving features file.
     * This is a sequence of (<var>x</var>, <var>y</var>, <var>z</var>) coordinate tuples
     * (the number of coordinates depends on {@link #trajectoryDimension}) in Cartesian CS.
     */
    private final double[] trajectory;

    /**
     * The start time of each coordinate tuple in {@link #trajectory}.
     */
    private final double[] startTimes;

    /**
     * Creates a math transform from the specified feature.
     *
     * @param  source   the URI specified in parameter, usually relative to an unspecified directory.
     * @param  crs      the reference system of the features.
     * @param  feature  the feature providing trajectory data.
     */
    TranslationByTrajectory(final URI source, final CoordinateReferenceSystem crs, final Feature feature) throws FactoryException {
        sourceFile = source;
        timeCRS = DefaultTemporalCRS.castOrCopy(CommonCRS.Temporal.TRUNCATED_JULIAN.crs());
        trajectoryDimension = crs.getCoordinateSystem().getDimension() - 1;     // Assume that time dimension is last.
        interpolationCRS = CRS.getComponentAt(crs, 0, trajectoryDimension);
        if (interpolationCRS == null) throw missingCRS();
        /*
         * The remaining code in this constructor use hard-coded property names and types.
         * If an assumption does not hold, IllegalArgumentException or ClassCastException
         * will be thrown. A more industrial code would do an analysis of feature type.
         */
        var attribute = (Attribute<?>) feature.getProperty("trajectory");
        var datetimes = attribute.characteristics().get("datetimes").getValues();
        var timeCount = datetimes.size();
        var geometry  = (LineString) attribute.getValue();
        startTimes = datetimes.stream().mapToDouble((t) -> timeCRS.toValue((Instant) t)).toArray();
        trajectory = new double[timeCount * trajectoryDimension];
        for (int i=0, c=0; i<timeCount; i++) {
            var coords = geometry.getCoordinateN(i);
            trajectory[c++] = coords.x;             // Heliocentric X
            trajectory[c++] = coords.y;             // Heliocentric Y
            trajectory[c++] = coords.z;             // Heliocentric Z
        }
//      yaw   = toArray(feature, "yaw",   timeCount);
//      pitch = toArray(feature, "pitch", timeCount);
//      roll  = toArray(feature, "roll",  timeCount);
    }

    /**
     * {@return the exception to throw when the interpolation CRS is missing}.
     */
    static FactoryException missingCRS() {
        return new FactoryException("Missing the interpolation CRS.");
    }

    /**
     * Returns the value of given property as an array of floating point numbers.
     *
     * <h4>Limitations</h4>
     * We should verify that the {@code MF:datetimes} characteristic of the specified property is the same
     * as the one of the trajectory. For simplicity reason, we don't do this verification in this demo.
     *
     * @param  feature        the feature from which to get the property.
     * @param  propertyName   name of the property to get.
     * @param  expectedCount  expected number of values in the array.
     * @return the property value as an array of floating points.
     * @throws IllegalArgumentException if the specified feature property is not found.
     * @throws ClassCastException if the property value is not of the expected type.
     * @throws ArrayIndexOutOfBoundsException if the property has more values than expected.
     */
    private static double[] toArray(final Feature feature, String propertyName, int expectedCount) {
        double[] values = new double[expectedCount];
        int actualCount = 0;
        for (Object item : (Iterable<?>) feature.getPropertyValue(propertyName)) {
            values[actualCount++] = ((Number) item).doubleValue();
        }
        // If the array is shorter than expected, repeat the last value.
        Arrays.fill(values, actualCount, expectedCount, values[actualCount - 1]);
        return values;
    }

    /**
     * Creates the complete transform, including conversion factors.
     */
    final MathTransform createCompleteTransform() throws NoninvertibleTransformException {
        final CoordinateSystem cs = interpolationCRS.getCoordinateSystem();
        final double[] factors = new double[trajectoryDimension + 1];
        factors[trajectoryDimension] = 1;
        for (int i=0; i<trajectoryDimension; i++) {
            factors[i] = Units.toStandardUnit(cs.getAxis(i).getUnit());
        }
        final MathTransform scale = MathTransforms.scale(factors);
        return MathTransforms.concatenate(scale.inverse(), this, scale);
    }

    /** {@return the total number of dimensions}. */
    @Override public int getSourceDimensions() {return trajectoryDimension + 1;}
    @Override public int getTargetDimensions() {return trajectoryDimension + 1;}

    /**
     * {@return the parameter values for this math transform}.
     */
    @Override
    public ParameterValueGroup getParameterValues() {
        final Parameters p = Parameters.castOrWrap(MovingFrame.PARAMETERS.createValue());
        p.getOrCreate(MovingFrame.FILE).setValue(sourceFile);
        return p;
    }

    /**
     * Computes a hash value for this transform when first needed.
     */
    @Override
    protected int computeHashCode() {
        return super.computeHashCode() + Objects.hash(sourceFile, interpolationCRS, timeCRS)
                + Arrays.hashCode(trajectory) + Arrays.hashCode(startTimes);
    }

    /**
     * Compares the specified object with this math transform for equality.
     */
    @Override
    public boolean equals(final Object object, final ComparisonMode mode) {
        if (super.equals(object, mode)) {
            final var other = (TranslationByTrajectory) object;
            if (Arrays   .equals    (trajectory,       other.trajectory) &&
                Arrays   .equals    (startTimes,       other.startTimes) &&
                Utilities.deepEquals(interpolationCRS, other.interpolationCRS, mode) &&
                Utilities.deepEquals(startTimes,       other.startTimes,       mode))
            {
                return mode.isIgnoringMetadata() || Objects.equals(sourceFile, other.sourceFile);
            }
        }
        return false;
    }

    /**
     * Transforms a single coordinate tuple in an array.
     * Input coordinates are Cartesian coordinates plus time.
     * Time is measured in days since Truncated Julian epoch for both input and output coordinates.
     *
     * @param  srcPts    the array containing the source coordinates (cannot be {@code null}).
     * @param  srcOff    the offset to the point to be transformed in the source array.
     * @param  dstPts    the array into which the transformed coordinates is returned.
     *                   May be {@code null} if only the derivative matrix is desired.
     * @param  dstOff    the offset to the location of the transformed point that is stored in the destination array.
     * @param  derivate  {@code true} for computing the derivative, or {@code false} if not needed.
     * @return the matrix of the transform derivative at the given source position,
     *         or {@code null} if the {@code derivate} argument is {@code false}.
     * @throws TransformException if the point cannot be transformed or
     *         if a problem occurred while calculating the derivative.
     */
    @Override
    public Matrix transform(final double[] srcPts, int srcOff,
                            final double[] dstPts, int dstOff, boolean derivate)
            throws TransformException
    {
        if (dstPts != null) {
            double x = srcPts[  srcOff];
            double y = srcPts[++srcOff];
            double z = srcPts[++srcOff];
            double t = srcPts[++srcOff];
            int i = Arrays.binarySearch(startTimes, t);
            if (i < 0) {
                i = ~i;
                if (i == 0 || i >= startTimes.length) {
                    throw new TransformException("Time out of range.");
                }
                /*
                 * We should interpolate `trajectory` values here.
                 * For keeping this demo simple, we skip that step.
                 */
            }
            i *= trajectoryDimension;
            x -= trajectory[  i];
            y -= trajectory[++i];
            z -= trajectory[++i];
            dstPts[  dstOff] = x;
            dstPts[++dstOff] = y;
            dstPts[++dstOff] = z;
            dstPts[++dstOff] = t;
        }
        if (derivate) {
            return Matrices.createIdentity(trajectoryDimension + 2);
        }
        return null;
    }

    /**
     * {@return the inverse of this transform}. The inverse uses a addition instead of subtraction.
     * But instead of inverting the sign of all trajectory coordinates, we instead invert the sign
     * of the source and target coordinates.
     */
    @Override
    public MathTransform inverse() {
        final double[] factors = new double[trajectoryDimension + 1];
        Arrays.fill(factors, 0, trajectoryDimension, -1);
        factors[trajectoryDimension] = 1;
        final MathTransform inverse = MathTransforms.scale(factors);
        return MathTransforms.concatenate(inverse, this, inverse);
    }

    // TODO: override the method for performing optimized concatenation.
}
