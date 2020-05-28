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
import java.util.Random;
import java.awt.Color;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;
import org.apache.sis.geometry.Envelope2D;
import org.apache.sis.geometry.ImmutableEnvelope;
import org.apache.sis.image.Interpolation;
import org.apache.sis.image.TiledImageMock;
import org.apache.sis.internal.coverage.j2d.TiledImage;
import org.apache.sis.internal.referencing.Formulas;
import org.apache.sis.referencing.CommonCRS;
import org.apache.sis.referencing.crs.HardCodedCRS;
import org.apache.sis.referencing.operation.HardCodedConversions;
import org.apache.sis.referencing.operation.matrix.Matrices;
import org.apache.sis.referencing.operation.matrix.MatrixSIS;
import org.apache.sis.referencing.operation.transform.MathTransforms;
import org.apache.sis.referencing.operation.transform.TransformSeparator;
import org.apache.sis.test.TestUtilities;
import org.apache.sis.test.DependsOn;
import org.apache.sis.test.DependsOnMethod;
import org.apache.sis.test.TestCase;
import org.junit.Test;

import static org.opengis.referencing.datum.PixelInCell.CELL_CENTER;
import static org.apache.sis.test.FeatureAssert.*;


/**
 * Tests the {@link ResampledGridCoverage} implementation.
 * The tests in this class does not verify interpolation results
 * (this is {@link org.apache.sis.image.ResampledImageTest} job).
 * Instead it focus on the grid geometry inferred by the operation.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
@DependsOn(org.apache.sis.image.ResampledImageTest.class)
public final strictfp class ResampledGridCoverageTest extends TestCase {
    /**
     * The random number generator used for generating some grid coverage values.
     * Created only if needed.
     */
    private Random random;

    /**
     * Arbitrary non-zero pixel coordinates for image origin.
     */
    private int minX = -2, minY = -2;

    /**
     * Arbitrary non-zero grid coordinate for the <var>z</var> and <var>t</var> dimensions.
     */
    private int gridZ, gridT;

    /**
     * Creates a small grid coverage with arbitrary data. The rendered image will
     * have only one tile since testing tiling is not the purpose of this class.
     * This simple coverage is two-dimensional.
     */
    private GridCoverage2D createCoverage2D() {
        random = TestUtilities.createRandomNumberGenerator();
        final int width  = random.nextInt(8) + 3;
        final int height = random.nextInt(8) + 3;
        minX = random.nextInt(32) - 10;
        minY = random.nextInt(32) - 10;
        final TiledImageMock image = new TiledImageMock(
                DataBuffer.TYPE_USHORT, 2,      // dataType and numBands
                minX,  minY,
                width, height,                  // Image size
                width, height,                  // Tile size
                random.nextInt(32) - 10,        // minTileX
                random.nextInt(32) - 10);       // minTileY
        image.validate();
        image.initializeAllTiles(0);
        final int x = random.nextInt(32) - 10;
        final int y = random.nextInt(32) - 10;
        final GridGeometry gg = new GridGeometry(
                new GridExtent(null, new long[] {x, y}, new long[] {x+width, y+height}, false),
                new Envelope2D(HardCodedCRS.WGS84, 20, 15, 60, 62));
        return new GridCoverage2D(gg, null, image);
    }

    /**
     * Size of a quadrant in the coverage created by {@link #createCoverageND(boolean)}.
     * The total image width and height are {@code 2*Q}.
     */
    private static final int QS = 3;

    /**
     * Creates a coverage in {@linkplain HardCodedCRS#WGS84_3D OGC:CRS:84 + elevation} reference system.
     * If the {@code withTime} argument is {@code true}, then the coverage will also include a temporal
     * dimension. The grid coverage characteristics are:
     * <ul>
     *   <li>Dimension is 6×6.</li>
     *   <li>Grid extent is zero-based.</li>
     *   <li>Envelope is arbitrary but stable (no random values).</li>
     *   <li>Display oriented (origin is in upper-left corner).</li>
     *   <li>3 byte bands for RGB coloration.</li>
     *   <li>Each quarter of the overall image is filled with a plain color:
     *     <table style="color:white;border-collapse:collapse;">
     *       <tbody style="border:none">
     *         <tr>
     *           <td style="width:50%; background-color:black">Black</td>
     *           <td style="width:50%; background-color:red">Red</td>
     *         </tr>
     *         <tr>
     *           <td style="width:50%; background-color:green">Green</td>
     *           <td style="width:50%; background-color:blue">Blue</td>
     *         </tr>
     *       </tbody>
     *     </table>
     *   </li>
     * </ul>
     *
     * @param  withTime  {@code false} for a three-dimensional coverage, or {@code true} for adding a temporal dimension.
     * @return a new three- or four-dimensional RGB Grid Coverage.
     */
    private GridCoverage createCoverageND(final boolean withTime) {
        random = TestUtilities.createRandomNumberGenerator();
        final BufferedImage image = new BufferedImage(2*QS, 2*QS, BufferedImage.TYPE_3BYTE_BGR);
        final int[] color = new int[QS*QS];
        /* Upper-left  quarter */ // Keep default value, which is black.
        /* Upper-right quarter */ Arrays.fill(color, Color.RED  .getRGB()); image.setRGB(QS,  0, QS, QS, color, 0, QS);
        /* Lower-left  quarter */ Arrays.fill(color, Color.GREEN.getRGB()); image.setRGB( 0, QS, QS, QS, color, 0, QS);
        /* Lower-right quarter */ Arrays.fill(color, Color.BLUE .getRGB()); image.setRGB(QS, QS, QS, QS, color, 0, QS);
        /*
         * Create an image with origin between -2 and +2. We use a random image location for more
         * complete testing, but actually the tests in this class are independent of image origin.
         * Note that grid extent origin does not need to be the same than image origin.
         */
        minX = random.nextInt(5) - 2;
        minY = random.nextInt(5) - 2;
        GridGeometry gg = createGridGeometryND(withTime ? HardCodedCRS.WGS84_4D : HardCodedCRS.WGS84_3D, 0, 1, 2, 3, false);
        final TiledImage shiftedImage = new TiledImage(
                image.getColorModel(),
                image.getWidth(), image.getHeight(),        // Image size
                random.nextInt(32) - 10,                    // minTileX
                random.nextInt(32) - 10,                    // minTileY
                image.getRaster().createTranslatedChild(minX, minY));
        return new GridCoverage2D(gg, null, shiftedImage);
    }

    /**
     * Creates the grid geometry associated with {@link #createCoverageND(boolean)}, optionally with swapped
     * horizontal axes and flipped Y axis. The given CRS shall have 3 or 4 dimensions.
     *
     * @param  crs    the coordinate reference system to assign to the grid geometry.
     * @param  x      dimension of <var>x</var> coordinates (typically 0).
     * @param  y      dimension of <var>y</var> coordinates (typically 1).
     * @param  z      dimension of <var>z</var> coordinates (typically 2).
     * @param  t      dimension of <var>t</var> coordinates (typically 3). Ignored if the CRS is not four-dimensional.
     * @param  flipY  whether to flip the <var>y</var> axis.
     */
    private GridGeometry createGridGeometryND(final CoordinateReferenceSystem crs,
            final int x, final int y, final int z, final int t, final boolean flipY)
    {
        final int dim = crs.getCoordinateSystem().getDimension();
        final long[] lower = new long[dim];
        final long[] upper = new long[dim];
        Arrays.fill(upper, StrictMath.min(x,y), StrictMath.max(x,y)+1, 2*QS - 1);
        final MatrixSIS gridToCRS = Matrices.createIdentity(dim + 1);
        gridToCRS.setElement(x, x,    44./(2*QS));  // X scale
        gridToCRS.setElement(x, dim, -50./(2*QS));  // X translation
        gridToCRS.setElement(y, y,   -3.5);         // Y scale
        gridToCRS.setElement(y, dim, -0.75);        // Y translation
        gridToCRS.setElement(z, dim, -100);
        lower[z] = upper[z] = gridZ = 7;            // Arbitrary non-zero position in the grid.
        if (t < dim) {
            gridToCRS.setElement(t, dim, 48055);
            lower[t] = upper[t] = gridT = 12;
        }
        if (flipY) {
            gridToCRS.setElement(y, y,     3.5);    // Inverse sign.
            gridToCRS.setElement(y, dim, -18.25);
        }
        return new GridGeometry(new GridExtent(null, lower, upper, true),
                    CELL_CENTER, MathTransforms.linear(gridToCRS), crs);
    }

    /**
     * Verifies that the given target coverage has the same pixel values than the source coverage.
     * This method opportunistically verifies that the target {@link GridCoverage} instance has a
     * {@link GridCoverage#render(GridExtent)} implementation conforms to the specification, i.e.
     * that requesting only a sub-area results in an image where pixel coordinate (0,0) corresponds
     * to cell coordinates in the lower corner of specified {@code sliceExtent}.
     */
    private void assertContentEquals(final GridCoverage source, final GridCoverage target) {
        final int tx = random.nextInt(3);
        final int ty = random.nextInt(3);
        final GridExtent sourceExtent = source.gridGeometry.getExtent();
        final int newWidth   = (int) sourceExtent.getSize(0) - tx;
        final int newHeight  = (int) sourceExtent.getSize(1) - ty;
        GridExtent subExtent = new GridExtent(
                (int) sourceExtent.getLow(0) + tx,
                (int) sourceExtent.getLow(1) + ty,
                newWidth,
                newHeight
        );
        assertPixelsEqual(source.render(null), new Rectangle(tx, ty, newWidth, newHeight),
                          target.render(subExtent), new Rectangle(newWidth, newHeight));
    }

    /**
     * Returns a resampled coverage using processor with default configuration.
     * We use processor instead than instantiating {@link ResampledGridCoverage} directly in order
     * to test {@link GridCoverageProcessor#resample(GridCoverage, GridGeometry)} method as well.
     */
    private static GridCoverage resample(final GridCoverage source, final GridGeometry target) throws TransformException {
        final GridCoverageProcessor processor = new GridCoverageProcessor();
        processor.setInterpolation(Interpolation.NEAREST);
        return processor.resample(source, target);
    }

    /**
     * Tests application of an identity transform computed from an explicitly given "grid to CRS" transform.
     * We expect the source coverage to be returned unchanged.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testExplicitIdentity() throws TransformException {
        final GridCoverage2D source = createCoverage2D();
        GridGeometry gg = source.getGridGeometry();
        gg = new GridGeometry(null, CELL_CENTER, gg.getGridToCRS(CELL_CENTER), gg.getCoordinateReferenceSystem());
        final GridCoverage target = resample(source, gg);
        assertSame("Identity transform should result in same coverage.", source, target);
        assertContentEquals(source, target);
    }

    /**
     * Tests application of an identity transform without specifying explicitly the desired grid geometry.
     * This test is identical to {@link #testExplicitIdentity()} except that the "grid to CRS" transform
     * specified to the {@code resample(…)} operation is null.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    @DependsOnMethod("testExplicitIdentity")
    public void testImplicitIdentity() throws TransformException {
        final GridCoverage2D source = createCoverage2D();
        GridGeometry gg = source.getGridGeometry();
        gg = new GridGeometry(null, CELL_CENTER, null, gg.getCoordinateReferenceSystem());
        final GridCoverage target = resample(source, gg);
        assertSame("Identity transform should result in same coverage.", source, target);
        assertContentEquals(source, target);
    }

    /**
     * Tests application of axis swapping in a two-dimensional coverage.
     * This test verifies the envelope of resampled coverage.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testAxisSwap() throws TransformException {
        final GridCoverage2D source = createCoverage2D();
        GridGeometry gg = new GridGeometry(null, CELL_CENTER, null, HardCodedCRS.WGS84_φλ);
        final GridCoverage target = resample(source, gg);
        /*
         * We expect the same image since `ResampledGridCoverage` should have been
         * able to apply the operation with only a change of `gridToCRS` transform.
         */
        assertNotSame(source, target);
        assertSame(unwrap(source.render(null)),
                   unwrap(target.render(null)));
        /*
         * As an easy way to check that axis swapping has happened, check the envelopes.
         */
        final ImmutableEnvelope se = source.getGridGeometry().envelope;
        final ImmutableEnvelope te = target.getGridGeometry().envelope;
        assertEquals(se.getLower(0), te.getLower(1), Formulas.ANGULAR_TOLERANCE);
        assertEquals(se.getLower(1), te.getLower(0), Formulas.ANGULAR_TOLERANCE);
        assertEquals(se.getUpper(0), te.getUpper(1), Formulas.ANGULAR_TOLERANCE);
        assertEquals(se.getUpper(1), te.getUpper(0), Formulas.ANGULAR_TOLERANCE);
    }

    /**
     * Unwraps the given image if it is an instance of {@link ReshapedImage}.
     */
    private static RenderedImage unwrap(final RenderedImage image) {
        assertEquals("GridCoverage.render(null) should have their origin at (0,0).", 0, image.getMinX());
        assertEquals("GridCoverage.render(null) should have their origin at (0,0).", 0, image.getMinY());
        return (image instanceof ReshapedImage) ? ((ReshapedImage) image).image : image;
    }

    /**
     * Tests application of axis swapping in a three-dimensional coverage, together with an axis flip.
     * This test verifies that the pixel values of resampled coverage are found in expected quadrant.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    @DependsOnMethod("testAxisSwap")
    public void testAxisSwapAndFlip() throws TransformException {
        final GridCoverage  source      = createCoverageND(false);
        final GridGeometry  target      = createGridGeometryND(CommonCRS.WGS84.geographic3D(), 1, 0, 2, 3, true);
        final GridCoverage  result      = resample(source, target);
        final RenderedImage sourceImage = source.render(null);
        final RenderedImage targetImage = result.render(null);
        assertEquals(target, result.getGridGeometry());
        assertEquals("minX", 0, sourceImage.getMinX());                     // As per GridCoverage.render(…) contract.
        assertEquals("minY", 0, sourceImage.getMinY());
        assertEquals("minX", 0, targetImage.getMinX());
        assertEquals("minY", 0, targetImage.getMinY());
        assertPixelsEqual(sourceImage, new Rectangle( 0, QS, QS, QS),
                          targetImage, new Rectangle( 0,  0, QS, QS));      // Green should be top-left.
        assertPixelsEqual(sourceImage, new Rectangle( 0,  0, QS, QS),
                          targetImage, new Rectangle(QS,  0, QS, QS));      // Black should be upper-right.
        assertPixelsEqual(sourceImage, new Rectangle(QS, QS, QS, QS),
                          targetImage, new Rectangle( 0, QS, QS, QS));      // Blue should be lower-left.
        assertPixelsEqual(sourceImage, new Rectangle(QS,  0, QS, QS),
                          targetImage, new Rectangle(QS, QS, QS, QS));      // Red should be lower-right.
    }

    /**
     * Tests an operation moving the dimension of temporal axis in a four-dimensional coverage.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testTemporalAxisMoved() throws TransformException {
        final GridCoverage source = createCoverageND(true);
        final GridGeometry target = createGridGeometryND(HardCodedCRS.TIME_WGS84, 1, 2, 3, 0, false);
        final GridCoverage result = resample(source, target);
        assertAxisDirectionsEqual("Expected (t,λ,φ,H) axes.",
                result.getGridGeometry().getCoordinateReferenceSystem().getCoordinateSystem(),
                AxisDirection.FUTURE, AxisDirection.EAST, AxisDirection.NORTH, AxisDirection.UP);

        assertPixelsEqual(source.render(null), null, result.render(null), null);
    }

    /**
     * Tests resampling in a sub-region specified by a grid extent. This method uses a three-dimensional coverage,
     * which implies that this method also tests the capability to identify which slice needs to be resampled.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testSubGridExtent() throws TransformException {
        final GridCoverage source     = createCoverageND(false);
        final GridGeometry sourceGeom = source.getGridGeometry();
        final GridGeometry targetGeom = new GridGeometry(
                new GridExtent(null, new long[] {2, 2, gridZ}, new long[] {5, 5, gridZ}, true),
                CELL_CENTER, sourceGeom.gridToCRS,
                sourceGeom.getCoordinateReferenceSystem());

        final GridCoverage result = resample(source, targetGeom);
        assertEquals(targetGeom, result.getGridGeometry());
        /*
         * Verify that the target coverage contains all pixel values of the source coverage.
         * Iteration over source pixels needs to be restricted to the `targetGeom` extent.
         */
        final RenderedImage sourceImage = source.render(null);
        RenderedImage targetImage = result.render(null);
        assertPixelsEqual(sourceImage, new Rectangle(2, 2, 4, 4),
                          targetImage, null);
        /*
         * Verify GridCoverage.render(GridExtent) contract: the origin of the returned image
         * shall be the lower-left corner of `sliceExtent`, which is (3,3) in this test.
         */
        targetImage = result.render(new GridExtent(3, 3, 2, 2));
        assertPixelsEqual(sourceImage, new Rectangle(3, 3, 2, 2),
                          targetImage, new Rectangle(0, 0, 2, 2));
    }

    /**
     * Tests resampling in a sub-region specified by a grid extent spanning a single column.
     * When trying to optimize resampling by dropping dimensions, it can happen that transform dimensions
     * are reduced to 1D. However, it is a problem for image case which requires 2D coordinates.
     * So we must ensure that resample conversion keeps at least two dimensions.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testSubGridExtentColumnar() throws TransformException {
        final GridCoverage2D source   = createCoverage2D();
        final GridGeometry sourceGeom = source.getGridGeometry();
        final GridExtent sourceExtent = sourceGeom.getExtent();
        final GridExtent targetExtent = new GridExtent(null,
                new long[] {sourceExtent.getLow(0), sourceExtent.getLow (1)},
                new long[] {sourceExtent.getLow(0), sourceExtent.getHigh(1)}, true);
        final GridGeometry targetGeom = new GridGeometry(
                targetExtent, CELL_CENTER,
                sourceGeom.getGridToCRS(CELL_CENTER),
                sourceGeom.getCoordinateReferenceSystem());

        final GridCoverage result = resample(source, targetGeom);
        final int height = (int) targetExtent.getSize(1);
        assertPixelsEqual(source.render(null), new Rectangle(0, 0, 1, height), result.render(null), null);
    }

    /**
     * Tests resampling in a sub-region specified by an envelope.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testSubGeographicArea() throws TransformException {
        final GridCoverage2D source = createCoverage2D();             // Envelope2D(20, 15, 60, 62)
        GridGeometry gg = new GridGeometry(null, new Envelope2D(HardCodedCRS.WGS84, 18, 20, 17, 31));
        final GridCoverage target = resample(source, gg);
        final GridExtent sourceExtent = source.getGridGeometry().getExtent();
        final GridExtent targetExtent = target.getGridGeometry().getExtent();
        assertTrue(sourceExtent.getSize(0) > targetExtent.getSize(0));
        assertTrue(sourceExtent.getSize(1) > targetExtent.getSize(1));
    }

    /**
     * Tests application of a non-linear transform.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testReprojection() throws TransformException {
        final GridCoverage2D source = createCoverage2D();
        GridGeometry gg = new GridGeometry(null, CELL_CENTER, null, HardCodedConversions.mercator());
        final GridCoverage target = resample(source, gg);
        assertTrue("GridExtent.startsAtZero", target.getGridGeometry().getExtent().startsAtZero());
        /*
         * Mercator projection does not change pixel width, but change pixel height.
         */
        final GridExtent sourceExtent = source.getGridGeometry().getExtent();
        final GridExtent targetExtent = target.getGridGeometry().getExtent();
        assertEquals(sourceExtent.getSize(0),   targetExtent.getSize(0));
        assertTrue  (sourceExtent.getSize(1) <= targetExtent.getSize(1));
    }

    /**
     * Tests application of a three-dimensional transform which can not be reduced to a two-dimensional transform.
     * It happens for example when transformation of <var>x</var> or <var>y</var> coordinate depends on <var>z</var>
     * coordinate value. In such case we can not separate the 3D transform into (2D + 1D) transforms. This method
     * verifies that {@link ResampledGridCoverage} nevertheless manages to do its work even in that situation.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void testNonSeparableGridToCRS() throws TransformException {
        final GridCoverage source = createCoverageND(false);
        final MatrixSIS nonSeparableMatrix = Matrices.createDiagonal(4, 4);
        nonSeparableMatrix.setElement(0, 2, 1);     // Make X dependent of Z.
        nonSeparableMatrix.setElement(1, 2, 1);     // Make Y dependent of Z.
        final MathTransform nonSeparableG2C = MathTransforms.concatenate(
                source.getGridGeometry().getGridToCRS(CELL_CENTER),
                MathTransforms.linear(nonSeparableMatrix));
        {
            /*
             * The test in this block is not a `ResampleGridCoverage` test, but rather a
             * check for a condition that we need for the test performed in this method.
             */
            final TransformSeparator separator = new TransformSeparator(nonSeparableG2C);
            separator.addSourceDimensions(0, 1);
            separator.addTargetDimensions(0, 1);
            try {
                final MathTransform separated = separator.separate();
                fail("Test requires a non-separable transform, but separation succeed: " + separated);
            } catch (FactoryException e) {
                // Successful check.
            }
        }
        final GridGeometry targetGeom = new GridGeometry(
                null,           // Let the resample operation compute the extent automatically.
                CELL_CENTER, nonSeparableG2C,
                source.getCoordinateReferenceSystem());
        /*
         * Real test is below (above code was only initialization).
         * Target image should be 6×6 pixels, like source image.
         */
        final GridCoverage result = resample(source, targetGeom);
        assertPixelsEqual(source.render(null), null, result.render(null), null);
    }

    /**
     * Tests the addition of a temporal axis. The value to insert in the temporal coordinate can be computed
     * from the four-dimensional "grid to CRS" transform given in argument to the {@code resample(…)} method,
     * combined with the source grid extent.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    public void crs3D_to_crs4D() throws TransformException {
        final GridCoverage source3D = createCoverageND(false);
        final GridGeometry target4D = createGridGeometryND(HardCodedCRS.WGS84_4D, 0, 1, 2, 3, false);
        final GridCoverage result   = resample(source3D, target4D);
        assertEquals(target4D, result.getGridGeometry());
        assertPixelsEqual(source3D.render(null), null, result.render(null), null);
    }

    /**
     * Tests the removal of temporal axis.
     *
     * @throws TransformException if some coordinates can not be transformed to the target grid geometry.
     */
    @Test
    @org.junit.Ignore("To debug")
    public void crs4D_to_crs3D() throws TransformException {
        final GridGeometry target3D = createGridGeometryND(HardCodedCRS.WGS84_3D, 0, 1, 2, 3, false);
        final GridCoverage source4D = createCoverageND(true);
        final GridCoverage result   = resample(source4D, target3D);
        assertEquals(target3D, result.getGridGeometry());
        assertPixelsEqual(source4D.render(null), null, result.render(null), null);
    }

    /**
     * Returns an image with only the queries part of the given image.
     * This is an helper tools which can be invoked during debugging
     * session in IDE capable to display images.
     *
     * <p><b>Usage:</b> Add a new watch calling this method on wanted image.</p>
     *
     * <p><b>Limitations:</b>
     * <ul>
     *   <li>If given image color-model is null, this method assumes 3 byte/RGB image.</li>
     *   <li>Works only with single-tile images.</li>
     * </ul>
     *
     * @param source  the image to display.
     * @param extent  if non-null, crop rendering to the rectangle defined by given extent,
     *                assuming extent low coordinate matches source image (0,0) coordinate.
     * @return the image directly displayable through debugger.
     */
    private static BufferedImage debug(final RenderedImage source, final GridExtent extent) {
        Raster tile = source.getTile(source.getMinTileX(), source.getMinTileY());
        final int width, height;
        if (extent == null) {
            tile   = tile.createTranslatedChild(0, 0);
            width  = tile.getWidth();
            height = tile.getHeight();
        } else {
            width  = StrictMath.toIntExact(extent.getSize(0));
            height = StrictMath.toIntExact(extent.getSize(1));
            tile   = tile.createChild(0, 0, width, height, 0, 0, null);
        }
        final BufferedImage view;
        if (source.getColorModel() == null) {
            view = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
            view.getRaster().setRect(tile);
        } else {
            final WritableRaster wr = tile.createCompatibleWritableRaster(0, 0, width, height);
            wr.setRect(tile);
            view = new BufferedImage(source.getColorModel(), wr, false, null);
        }
        return view;
    }
}
