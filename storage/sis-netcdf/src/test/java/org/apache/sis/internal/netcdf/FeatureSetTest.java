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

import java.awt.Shape;
import java.awt.geom.PathIterator;
import java.util.Iterator;
import java.util.Collection;
import java.io.IOException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.test.DependsOn;
import org.junit.Test;

import static org.junit.Assert.*;

// Branch-dependent imports
import org.opengis.feature.Feature;
import org.opengis.feature.FeatureType;
import org.opengis.feature.PropertyType;
import org.opengis.feature.AttributeType;
import org.opengis.test.dataset.TestData;


/**
 * Tests the {@link FeatureSet} implementation. The default implementation uses the UCAR library,
 * which is is our reference implementation. Subclass overrides {@link #createDecoder(TestData)}
 * method in order to test a different implementation.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
@DependsOn(VariableTest.class)
public strictfp class FeatureSetTest extends TestCase {
    /**
     * Type of the features read from the netCDF file.
     */
    private FeatureType type;

    /**
     * Index of the feature to verify.
     */
    private int featureIndex;

    /**
     * Tests {@link FeatureSet} with a moving features file.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testMovingFeatures() throws IOException, DataStoreException {
        final Object lock = new Object();
        final FeatureSet[] features;
        synchronized (lock) {
            features = FeatureSet.create(selectDataset(TestData.MOVING_FEATURES), lock);
        }
        assertEquals(1, features.length);
        type = features[0].getType();
        verifyType(type.getProperties(false).iterator());
        features[0].features(false).forEach(this::verifyInstance);
    }

    /**
     * Verifies that given properties are the expected ones
     * for {@link TestData#MOVING_FEATURES} feature type.
     */
    private static void verifyType(final Iterator<? extends PropertyType> it) {
        assertEquals("sis:identifier", it.next().getName().toString());
        assertEquals("sis:envelope",   it.next().getName().toString());
        assertEquals("sis:geometry",   it.next().getName().toString());

        AttributeType<?> at = (AttributeType<?>) it.next();
        assertEquals("features", at.getName().toString());

        at = (AttributeType<?>) it.next();
        assertEquals("trajectory", at.getName().toString());
        assertEquals(Shape.class, at.getValueClass());

        at = (AttributeType<?>) it.next();
        assertEquals("stations", at.getName().toString());
        assertEquals(String.class, at.getValueClass());

        assertFalse(it.hasNext());
    }

    /**
     * Verifies the given feature instance.
     */
    private void verifyInstance(final Feature instance) {
        assertSame(type, instance.getType());
        final float[] longitudes, latitudes;
        final String[] stations;
        final String identifier;
        switch (featureIndex++) {
            case 0: {
                identifier = "a4078a16";
                longitudes = new float[] {139.622715f, 139.696899f, 139.740440f, 139.759640f, 139.763328f, 139.766084f};
                latitudes  = new float[] { 35.466188f,  35.531328f,  35.630152f,  35.665498f,  35.675069f,  35.681382f};
                stations   = new String[] {
                    "Yokohama", "Kawasaki", "Shinagawa", "Shinbashi", "Yurakucho", "Tokyo"
                };
                break;
            }
            case 1: {
                identifier = "1e146c16";
                longitudes = new float[] {139.700258f, 139.730667f, 139.763786f, 139.774219f};
                latitudes  = new float[] { 35.690921f,  35.686014f,  35.699855f,  35.698683f};
                stations   = new String[] {
                    "Shinjuku", "Yotsuya", "Ochanomizu", "Akihabara"
                };
                break;
            }
            case 2: {
                identifier = "f50ff004";
                longitudes = new float[] {139.649867f, 139.665652f, 139.700258f};
                latitudes  = new float[] { 35.705385f,  35.706032f,  35.690921f};
                stations   = new String[] {
                    "Koenji", "Nakano", "Shinjuku"
                };
                break;
            }
            default: {
                fail("Unexpected feature instance.");
                return;
            }
        }
        assertEquals("identifier", identifier, instance.getPropertyValue("features"));
        asserLineStringEquals((Shape) instance.getPropertyValue("trajectory"), longitudes, latitudes);
        assertArrayEquals("stations", stations, ((Collection<?>) instance.getPropertyValue("stations")).toArray());
    }

    /**
     * Asserts the the given shape is a line string with the following coordinates.
     *
     * @param  trajectory  the shape to verify.
     * @param  x           expected X coordinates.
     * @param  y           expected Y coordinates.
     */
    private static void asserLineStringEquals(final Shape trajectory, final float[] x, final float[] y) {
        assertEquals(x.length, y.length);
        final PathIterator it = trajectory.getPathIterator(null);
        final float[] point = new float[2];
        for (int i=0; i < x.length; i++) {
            assertFalse(it.isDone());
            assertEquals(i == 0 ? PathIterator.SEG_MOVETO : PathIterator.SEG_LINETO, it.currentSegment(point));
            assertEquals("x", x[i], point[0], STRICT);
            assertEquals("y", y[i], point[1], STRICT);
            it.next();
        }
        assertTrue(it.isDone());
    }
}