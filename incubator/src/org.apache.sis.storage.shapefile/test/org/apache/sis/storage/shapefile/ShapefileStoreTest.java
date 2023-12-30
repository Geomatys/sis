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
package org.apache.sis.storage.shapefile;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.apache.sis.feature.builder.FeatureTypeBuilder;
import org.apache.sis.util.Utilities;
import org.apache.sis.filter.DefaultFilterFactory;
import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.referencing.CommonCRS;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.FeatureQuery;
import org.apache.sis.storage.FeatureSet;
import org.apache.sis.feature.internal.AttributeConvention;

// Test dependencies
import static org.junit.jupiter.api.Assertions.*;
import org.junit.Ignore;
import org.junit.Test;

// Specific to the main branch:
import org.apache.sis.feature.AbstractFeature;
import org.apache.sis.feature.DefaultFeatureType;
import org.apache.sis.feature.DefaultAttributeType;
import org.apache.sis.filter.Filter;


/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class ShapefileStoreTest {

    private static final GeometryFactory GF = new GeometryFactory();

    @Test
    public void testStream() throws URISyntaxException, DataStoreException {
        final URL url = ShapefileStoreTest.class.getResource("/org/apache/sis/storage/shapefile/point.shp");
        try (final ShapefileStore store = new ShapefileStore(Paths.get(url.toURI()))) {

            //check feature type
            final DefaultFeatureType type = store.getType();
            assertEquals("point", type.getName().toString());
            assertEquals(9, type.getProperties(true).size());
            assertNotNull(type.getProperty("sis:identifier"));
            assertNotNull(type.getProperty("sis:envelope"));
            assertNotNull(type.getProperty("sis:geometry"));
            final var geomProp    = (DefaultAttributeType) type.getProperty("geometry");
            final var idProp      = (DefaultAttributeType) type.getProperty("id");
            final var textProp    = (DefaultAttributeType) type.getProperty("text");
            final var integerProp = (DefaultAttributeType) type.getProperty("integer");
            final var floatProp   = (DefaultAttributeType) type.getProperty("float");
            final var dateProp    = (DefaultAttributeType) type.getProperty("date");
            assertEquals(Point.class, geomProp.getValueClass());
            assertEquals(Long.class, idProp.getValueClass());
            assertEquals(String.class, textProp.getValueClass());
            assertEquals(Long.class, integerProp.getValueClass());
            assertEquals(Double.class, floatProp.getValueClass());
            assertEquals(LocalDate.class, dateProp.getValueClass());

            try (Stream<AbstractFeature> stream = store.features(false)) {
                Iterator<AbstractFeature> iterator = stream.iterator();
                assertTrue(iterator.hasNext());
                AbstractFeature feature1 = iterator.next();
                assertEquals(1L, feature1.getPropertyValue("id"));
                assertEquals("text1", feature1.getPropertyValue("text"));
                assertEquals(10L, feature1.getPropertyValue("integer"));
                assertEquals(20.0, feature1.getPropertyValue("float"));
                assertEquals(LocalDate.of(2023, 10, 27), feature1.getPropertyValue("date"));
                Point pt1 = (Point) feature1.getPropertyValue("geometry");

                assertTrue(iterator.hasNext());
                AbstractFeature feature2 = iterator.next();
                assertEquals(2L, feature2.getPropertyValue("id"));
                assertEquals("text2", feature2.getPropertyValue("text"));
                assertEquals(40L, feature2.getPropertyValue("integer"));
                assertEquals(60.0, feature2.getPropertyValue("float"));
                assertEquals(LocalDate.of(2023, 10, 28), feature2.getPropertyValue("date"));
                Point pt2 = (Point) feature2.getPropertyValue("geometry");

                assertFalse(iterator.hasNext());
            }
        }
    }

    /**
     * Test optimized envelope filter.
     */
    @Test
    public void testEnvelopeFilter() throws URISyntaxException, DataStoreException {
        final URL url = ShapefileStoreTest.class.getResource("/org/apache/sis/storage/shapefile/point.shp");
        try (final ShapefileStore store = new ShapefileStore(Paths.get(url.toURI()))) {

            final DefaultFilterFactory<AbstractFeature, Object, Object> ff = DefaultFilterFactory.forFeatures();

            final GeneralEnvelope env = new GeneralEnvelope(CommonCRS.WGS84.normalizedGeographic());
            env.setRange(0, 2, 3);
            env.setRange(1, 42, 43);

            final FeatureQuery query = new FeatureQuery();
            query.setSelection(ff.bbox(ff.property("geometry"), env));
            FeatureSet featureset = store.subset(query);
            //ensure we obtained an optimized version
            assertEquals("org.apache.sis.storage.shapefile.ShapefileStore$AsFeatureSet", featureset.getClass().getName());

            try (Stream<AbstractFeature> stream = featureset.features(false)) {
                Iterator<AbstractFeature> iterator = stream.iterator();
                assertTrue(iterator.hasNext());
                AbstractFeature feature = iterator.next();
                assertEquals(2L, feature.getPropertyValue("id"));
                assertEquals("text2", feature.getPropertyValue("text"));
                assertEquals(40L, feature.getPropertyValue("integer"));
                assertEquals(60.0, feature.getPropertyValue("float"));
                assertEquals(LocalDate.of(2023, 10, 28), feature.getPropertyValue("date"));
                Point pt2 = (Point) feature.getPropertyValue("geometry");

                assertFalse(iterator.hasNext());
            }
        }
    }

    /**
     * Test optimized field selection.
     */
    @Test
    public void testFieldFilter() throws URISyntaxException, DataStoreException {
        final URL url = ShapefileStoreTest.class.getResource("/org/apache/sis/storage/shapefile/point.shp");
        try (final ShapefileStore store = new ShapefileStore(Paths.get(url.toURI()))) {
            final FeatureQuery query = new FeatureQuery();
            query.setProjection("text", "float");
            FeatureSet featureset = store.subset(query);
            //ensure we obtained an optimized version
            assertEquals("org.apache.sis.storage.shapefile.ShapefileStore$AsFeatureSet", featureset.getClass().getName());

            try (Stream<AbstractFeature> stream = featureset.features(false)) {
                Iterator<AbstractFeature> iterator = stream.iterator();
                assertTrue(iterator.hasNext());
                AbstractFeature feature1 = iterator.next();
                assertEquals("text1", feature1.getPropertyValue("text"));
                assertEquals(20.0, feature1.getPropertyValue("float"));

                assertTrue(iterator.hasNext());
                AbstractFeature feature2 = iterator.next();
                assertEquals("text2", feature2.getPropertyValue("text"));
                assertEquals(60.0, feature2.getPropertyValue("float"));

                assertFalse(iterator.hasNext());
            }
        }
    }

    @Test
    public void testFiles() throws URISyntaxException, DataStoreException {
        final URL url = ShapefileStoreTest.class.getResource("/org/apache/sis/storage/shapefile/point.shp");
        try (final ShapefileStore store = new ShapefileStore(Paths.get(url.toURI()))) {
            Path[] componentFiles = store.getComponentFiles();
            assertEquals(5, componentFiles.length);
            assertTrue(componentFiles[0].toString().endsWith("point.shp"));
            assertTrue(componentFiles[1].toString().endsWith("point.shx"));
            assertTrue(componentFiles[2].toString().endsWith("point.dbf"));
            assertTrue(componentFiles[3].toString().endsWith("point.prj"));
            assertTrue(componentFiles[4].toString().endsWith("point.cpg"));
        }
    }

    /**
     * Test creating a new shapefile.
     */
    @Test
    public void testCreate() throws URISyntaxException, DataStoreException, IOException {
        final Path folder = Files.createTempDirectory("shapefileTest");
        final Path temp = folder.resolve("test.shp");
        final String name = temp.getFileName().toString().split("\\.")[0];
        try (final ShapefileStore store = new ShapefileStore(temp)) {
            Path[] componentFiles = store.getComponentFiles();
            assertEquals(0, componentFiles.length);

            {//create type
                final DefaultFeatureType type = createType();
                store.updateType(type);
            }

            {//check files have been created
                componentFiles = store.getComponentFiles();
                assertEquals(5, componentFiles.length);
                assertTrue(componentFiles[0].toString().endsWith(name+".shp"));
                assertTrue(componentFiles[1].toString().endsWith(name+".shx"));
                assertTrue(componentFiles[2].toString().endsWith(name+".dbf"));
                assertTrue( componentFiles[3].toString().endsWith(name+".prj"));
                assertTrue(componentFiles[4].toString().endsWith(name+".cpg"));
            }

            {// check created type
                DefaultFeatureType type = store.getType();
                assertEquals(name, type.getName().toString());
                assertEquals(9, type.getProperties(true).size());
                assertNotNull(type.getProperty("sis:identifier"));
                assertNotNull(type.getProperty("sis:envelope"));
                assertNotNull(type.getProperty("sis:geometry"));
                final var geomProp = (DefaultAttributeType) type.getProperty("geometry");
                final var idProp = (DefaultAttributeType) type.getProperty("id");
                final var textProp = (DefaultAttributeType) type.getProperty("text");
                final var integerProp = (DefaultAttributeType) type.getProperty("integer");
                final var floatProp = (DefaultAttributeType) type.getProperty("float");
                final var dateProp = (DefaultAttributeType) type.getProperty("date");
                final DefaultAttributeType crsChar = (DefaultAttributeType) geomProp.characteristics().get(AttributeConvention.CRS);
                assertTrue(Utilities.equalsIgnoreMetadata(CommonCRS.WGS84.geographic(),crsChar.getDefaultValue()));
                assertEquals(Point.class, geomProp.getValueClass());
                assertEquals(Integer.class, idProp.getValueClass());
                assertEquals(String.class, textProp.getValueClass());
                assertEquals(Integer.class, integerProp.getValueClass());
                assertEquals(Double.class, floatProp.getValueClass());
                assertEquals(LocalDate.class, dateProp.getValueClass());
            }
        } finally {
            deleteDirectory(folder);
        }
    }

    /**
     * Test adding features to a shapefile.
     */
    @Test
    public void testAddFeatures() throws URISyntaxException, DataStoreException, IOException {
        final Path folder = Files.createTempDirectory("shapefileTest");
        final Path temp = folder.resolve("test.shp");
        try (final ShapefileStore store = new ShapefileStore(temp)) {
            DefaultFeatureType type = createType();
            store.updateType(type);
            type = store.getType();

            AbstractFeature feature1 = createFeature1(type);
            AbstractFeature feature2 = createFeature2(type);
            store.add(List.of(feature1, feature2).iterator());

            Object[] result = store.features(false).toArray();
            assertEquals(2, result.length);
            assertEquals(feature1, result[0]);
            assertEquals(feature2, result[1]);
        } finally {
            deleteDirectory(folder);
        }
    }

    /**
     * Test remove features from a shapefile.
     */
    @Test
    public void testRemoveFeatures() throws DataStoreException, IOException {
        final Path folder = Files.createTempDirectory("shapefileTest");
        final Path temp = folder.resolve("test.shp");
        try (final ShapefileStore store = new ShapefileStore(temp)) {
            DefaultFeatureType type = createType();
            store.updateType(type);
            type = store.getType();
            AbstractFeature feature1 = createFeature1(type);
            AbstractFeature feature2 = createFeature2(type);
            store.add(List.of(feature1, feature2).iterator());

            //remove first feature
            final DefaultFilterFactory<AbstractFeature, Object, Object> ff = DefaultFilterFactory.forFeatures();
            final Filter<AbstractFeature> filter = ff.equal(ff.property("id"), ff.literal(1));
            store.removeIf(filter);

            Object[] result = store.features(false).toArray();
            assertEquals(1, result.length);
            assertEquals(feature2, result[0]);
        } finally {
            deleteDirectory(folder);
        }
    }

    /**
     * Test replacing features in a shapefile.
     */
    @Test
    public void testReplaceFeatures() throws DataStoreException, IOException {
        final Path folder = Files.createTempDirectory("shapefileTest");
        final Path temp = folder.resolve("test.shp");
        try (final ShapefileStore store = new ShapefileStore(temp)) {
            DefaultFeatureType type = createType();
            store.updateType(type);
            type = store.getType();
            AbstractFeature feature1 = createFeature1(type);
            AbstractFeature feature2 = createFeature2(type);
            store.add(List.of(feature1, feature2).iterator());

            //remove first feature
            final DefaultFilterFactory<AbstractFeature, Object, Object> ff = DefaultFilterFactory.forFeatures();
            final Filter<AbstractFeature> filter = ff.equal(ff.property("id"), ff.literal(1));
            store.replaceIf(filter, new UnaryOperator<AbstractFeature>() {
                @Override
                public AbstractFeature apply(AbstractFeature feature) {
                    feature.setPropertyValue("id",45);
                    return feature;
                }
            });

            Object[] result = store.features(false).toArray();
            assertEquals(2, result.length);
            AbstractFeature f1 = (AbstractFeature) result[0];
            assertEquals(45, f1.getPropertyValue("id"));
            assertEquals(feature2, result[1]);
        } finally {
            deleteDirectory(folder);
        }
    }

    private static DefaultFeatureType createType() {
        final FeatureTypeBuilder ftb = new FeatureTypeBuilder();
        ftb.setName("test");
        ftb.addAttribute(Integer.class).setName("id");
        ftb.addAttribute(String.class).setName("text");
        ftb.addAttribute(Integer.class).setName("integer");
        ftb.addAttribute(Float.class).setName("float");
        ftb.addAttribute(LocalDate.class).setName("date");
        ftb.addAttribute(Point.class).setName("geometry").setCRS(CommonCRS.WGS84.geographic());
        return ftb.build();
    }

    private static AbstractFeature createFeature1(DefaultFeatureType type) {
        AbstractFeature feature = type.newInstance();
        feature.setPropertyValue("geometry", GF.createPoint(new Coordinate(10,20)));
        feature.setPropertyValue("id", 1);
        feature.setPropertyValue("text", "some text 1");
        feature.setPropertyValue("integer", 123);
        feature.setPropertyValue("float", 123.456);
        feature.setPropertyValue("date", LocalDate.of(2023, 5, 12));
        return feature;
    }

    private static AbstractFeature createFeature2(DefaultFeatureType type) {
        AbstractFeature feature = type.newInstance();
        feature.setPropertyValue("geometry", GF.createPoint(new Coordinate(30,40)));
        feature.setPropertyValue("id", 2);
        feature.setPropertyValue("text", "some text 2");
        feature.setPropertyValue("integer", 456);
        feature.setPropertyValue("float", 456.789);
        feature.setPropertyValue("date", LocalDate.of(2030, 6, 21));
        return feature;
    }

    private static void deleteDirectory(Path path) throws IOException{
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

}
