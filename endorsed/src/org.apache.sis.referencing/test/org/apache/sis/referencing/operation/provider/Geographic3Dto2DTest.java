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
package org.apache.sis.referencing.operation.provider;

import org.opengis.util.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransformFactory;
import org.opengis.referencing.operation.NoninvertibleTransformException;
import org.apache.sis.referencing.operation.transform.LinearTransform;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.parameter.Parameters;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.DependsOn;
import org.apache.sis.test.TestCase;

// Specific to the geoapi-3.1 and geoapi-4.0 branches:
import static org.opengis.test.Assert.assertMatrixEquals;


/**
 * Tests the {@link Geographic3Dto2D} class.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
@DependsOn(AffineTest.class)
public final class Geographic3Dto2DTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public Geographic3Dto2DTest() {
    }

    /**
     * Tests {@code Geographic3Dto2D.createMathTransform(…)}.
     *
     * @throws FactoryException should never happen.
     * @throws NoninvertibleTransformException should never happen.
     */
    @Test
    public void testCreateMathTransform() throws FactoryException, NoninvertibleTransformException {
        final Geographic3Dto2D provider = new Geographic3Dto2D();
        final MathTransform mt = provider.createMathTransform(null, null);
        assertSame(mt, provider.createMathTransform(null, null), "Expected cached instance.");
        /*
         * Verify the full matrix. Note that the longitude offset is expected to be in degrees.
         * This conversion from grad to degrees is specific to Apache SIS and may be revised in
         * future version. See org.apache.sis.referencing.operation package javadoc.
         */
        assertInstanceOf(LinearTransform.class, mt, "Shall be an affine transform.");
        assertMatrixEquals("Expected a Geographic 3D to 2D conversion.", Matrices.create(3, 4, new double[] {
                1, 0, 0, 0,
                0, 1, 0, 0,
                0, 0, 0, 1}), ((LinearTransform) mt).getMatrix(), STRICT);
        assertMatrixEquals("Expected a Geographic 2D to 3D conversion.", Matrices.create(4, 3, new double[] {
                1, 0, 0,
                0, 1, 0,
                0, 0, 0,
                0, 0, 1}), ((LinearTransform) mt.inverse()).getMatrix(), STRICT);
    }

    /**
     * Tests {@link Geographic3Dto2D#redimension(int, int)}.
     */
    @Test
    public void testRedimension() {
        final Geographic3Dto2D provider = new Geographic3Dto2D();
        assertSame  (provider,                    provider.redimension(3, 2));
        assertEquals(Geographic2Dto3D.class,      provider.redimension(2, 3).getClass());
        assertEquals(GeographicRedimension.class, provider.redimension(3, 3).getClass());
        assertEquals(GeographicRedimension.class, provider.redimension(2, 2).getClass());
    }

    /**
     * Creates a "Geographic 2D to 3D → Geocentric → Affine → Geographic → Geographic 3D to 2D" chain.
     * This method is used for integration tests.
     *
     * @param  factory  the math transform factory to use for creating and concatenating the transform.
     * @param  affine   the math transform for the operation in geocentric Cartesian domain.
     * @param  pv       the parameters for the operation in geographic coordinates.
     * @return the chain of transforms.
     * @throws FactoryException if an error occurred while creating a transform.
     *
     * @see GeocentricTranslationTest#createDatumShiftForGeographic2D(MathTransformFactory)
     */
    static MathTransform createDatumShiftForGeographic2D(final MathTransformFactory factory,
            final MathTransform affine, final Parameters pv) throws FactoryException
    {
        assertEquals(3, affine.getSourceDimensions());
        assertEquals(3, affine.getTargetDimensions());
        /*
         * Create a "Geographic to Geocentric" conversion with ellipsoid axis length units converted to metres
         * (the unit implied by SRC_SEMI_MAJOR) because it is the unit of Bursa-Wolf parameters that we created above.
         */
        Parameters step = Parameters.castOrWrap(factory.getDefaultParameters(GeographicToGeocentric.NAME));
        step.getOrCreate(MapProjection.SEMI_MAJOR).setValue(pv.doubleValue(GeocentricAffineBetweenGeographic.SRC_SEMI_MAJOR));
        step.getOrCreate(MapProjection.SEMI_MINOR).setValue(pv.doubleValue(GeocentricAffineBetweenGeographic.SRC_SEMI_MINOR));
        MathTransform toGeocentric = factory.createParameterizedTransform(step);
        assertEquals(3, toGeocentric.getSourceDimensions());
        assertEquals(3, toGeocentric.getTargetDimensions());

        final MathTransform reduce = factory.createParameterizedTransform(factory.getDefaultParameters("Geographic3D to 2D conversion"));
        assertEquals(3, reduce.getSourceDimensions());
        assertEquals(2, reduce.getTargetDimensions());
        try {
            toGeocentric = factory.createConcatenatedTransform(reduce.inverse(), toGeocentric);
        } catch (NoninvertibleTransformException e) {
            throw new FactoryException(e);
        }
        assertEquals(2, toGeocentric.getSourceDimensions());
        assertEquals(3, toGeocentric.getTargetDimensions());
        /*
         * Create a "Geocentric to Geographic" conversion with ellipsoid axis length units converted to metres
         * because this is the unit of the Geocentric CRS used above.
         */
        step = Parameters.castOrWrap(factory.getDefaultParameters(GeocentricToGeographic.NAME));
        step.getOrCreate(MapProjection.SEMI_MAJOR).setValue(pv.doubleValue(GeocentricAffineBetweenGeographic.TGT_SEMI_MAJOR));
        step.getOrCreate(MapProjection.SEMI_MINOR).setValue(pv.doubleValue(GeocentricAffineBetweenGeographic.TGT_SEMI_MINOR));
        MathTransform toGeographic = factory.createParameterizedTransform(step);
        assertEquals(3, toGeographic.getSourceDimensions());
        assertEquals(3, toGeographic.getTargetDimensions());

        toGeographic = factory.createConcatenatedTransform(toGeographic, reduce);
        assertEquals(3, toGeographic.getSourceDimensions());
        assertEquals(2, toGeographic.getTargetDimensions());
        /*
         * The  Geocentric → Affine → Geographic  chain.
         */
        return factory.createConcatenatedTransform(toGeocentric,
               factory.createConcatenatedTransform(affine, toGeographic));
    }
}
