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
package org.apache.sis.image;

import java.util.Optional;
import java.awt.Point;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.BandedSampleModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.WritableRenderedImage;
import org.apache.sis.util.Workaround;
import org.apache.sis.util.collection.FrequencySortedSet;
import org.apache.sis.internal.feature.Resources;
import org.apache.sis.internal.coverage.j2d.ImageLayout;
import org.apache.sis.internal.coverage.j2d.ImageUtilities;
import org.apache.sis.internal.coverage.j2d.ColorModelFactory;
import org.apache.sis.internal.coverage.MultiSourceArgument;
import org.apache.sis.coverage.grid.DisjointExtentException;


/**
 * Computes the bounds of a destination image which will combine many source images.
 * A combination may be an aggregation or an overlay of bands, depending on the image class.
 * All images are assumed to use the same pixel coordinate space: (<var>x</var>, <var>y</var>)
 * expressed in pixel coordinates should map to the same geospatial location in all images.
 *
 * <p>Instances of this class are temporary and used only during image construction.</p>
 *
 * @author  Alexis Manin (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.4
 *
 * @see ImageCombiner
 * @see BandAggregateImage
 *
 * @since 1.4
 */
final class MultiSourceLayout extends ImageLayout {
    /**
     * The source images. This is a copy of the user-specified array,
     * except that images associated to an empty set of bands are discarded.
     */
    private final RenderedImage[] sources;

    /**
     * The source images with only the user-specified bands.
     * Those images are views, the pixels are not copied.
     */
    final RenderedImage[] filteredSources;

    /**
     * Ordered (not necessarily sorted) indices of bands to select in each source image.
     * The length of this array is always equal to the length of the {@link #sources} array.
     * A {@code null} element means that all bands of the corresponding image should be used.
     * All non-null elements are non-empty and without duplicated values.
     */
    private final int[][] bandsPerSource;

    /**
     * The sample model of the combined image.
     * All {@linkplain BandedSampleModel#getBandOffsets() band offsets} are zeros and
     * all {@linkplain BandedSampleModel#getBankIndices() bank indices} are identity mapping.
     * This simplicity is needed by current implementation of {@link BandAggregateImage}.
     */
    final BandedSampleModel sampleModel;

    /**
     * The domain of pixel coordinates in the combined image. All sources images are assumed to use
     * the same pixel coordinate space, i.e. a pixel at coordinates (<var>x</var>, <var>y</var>) in
     * the combined image will contain values derived from pixels at the same coordinates in all
     * source images. It does <em>not</em> mean that all source images shall have the same bounds.
     */
    final Rectangle domain;

    /**
     * Index of the first tile. Other tile matrix properties such as the number of tiles and the grid offsets
     * are derived from those values together with the {@linkplain #domain}. Contrarily to pixel coordinates,
     * the tile coordinate space does not need to be the same for all images.
     *
     * @see #getMinTile()
     */
    final int minTileX, minTileY;

    /**
     * Whether to use the preferred tile size exactly as specified, without trying to compute a better size.
     * This field may be {@code true} if the tiles of the destination image are at exact same location than
     * the tiles of a source image having the preferred tile size. In such case, keeping the same size will
     * reduce the amount of tiles requested in that source image.
     */
    private final boolean exactTileSize;

    /**
     * Computes the layout of an image combining all the specified source images.
     * The optional {@code bandsPerSource} argument specifies the bands to select in each source images.
     * That array can be {@code null} for selecting all bands in all source images,
     * or may contain {@code null} elements for selecting all bands of the corresponding image.
     * An empty array element (i.e. zero band to select) discards the corresponding source image.
     *
     * <p>This static method is a workaround for RFE #4093999
     * ("Relax constraint on placement of this()/super() call in constructors").
     * This method may become the constructor after JEP 8300786 is available.</p>
     *
     * @param  sources         images to combine, in order.
     * @param  bandsPerSource  bands to use for each source image, in order. May contain {@code null} elements.
     * @param  allowSharing    whether to allow the sharing of data buffers (instead of copying) if possible.
     * @throws IllegalArgumentException if there is an incompatibility between some source images
     *         or if some band indices are duplicated or outside their range of validity.
     */
    @Workaround(library="JDK", version="1.8")
    static MultiSourceLayout create(RenderedImage[] sources, int[][] bandsPerSource, boolean allowSharing) {
        final var aggregate = new MultiSourceArgument<RenderedImage>(sources, bandsPerSource);
        aggregate.identityAsNull();
        aggregate.unwrap(BandAggregateImage::unwrap);
        aggregate.validate(ImageUtilities::getNumBands);

        sources            = aggregate.sources();
        bandsPerSource     = aggregate.bandsPerSource();
        Rectangle domain   = null;          // Nullity check used for telling when the first image is processed.
        int scanlineStride = 0;
        int tileWidth      = 0;
        int tileHeight     = 0;
        int tileAlignX     = 0;
        int tileAlignY     = 0;
        int commonDataType = DataBuffer.TYPE_UNDEFINED;
        for (final RenderedImage source : sources) {
            /*
             * Ensure that all images use the same data type. This is mandatory.
             * If in addition all images use the same pixel and scanline stride,
             * we may be able to share their buffers instead of copying values.
             */
            final SampleModel sm = source.getSampleModel();
            if (allowSharing && (allowSharing = (sm instanceof ComponentSampleModel))) {
                final ComponentSampleModel csm = (ComponentSampleModel) sm;
                if (allowSharing = (csm.getPixelStride() == 1)) {
                    allowSharing &= scanlineStride == (scanlineStride = csm.getScanlineStride());
                    allowSharing &= tileWidth      == (tileWidth      = source.getTileWidth());
                    allowSharing &= tileHeight     == (tileHeight     = source.getTileHeight());
                    allowSharing &= tileAlignX     == (tileAlignX     = Math.floorMod(source.getTileGridXOffset(), tileWidth));
                    allowSharing &= tileAlignY     == (tileAlignY     = Math.floorMod(source.getTileGridYOffset(), tileHeight));
                    allowSharing |= (domain == null);
                }
            }
            final int dataType = sm.getDataType();
            if (domain == null) {
                domain = ImageUtilities.getBounds(source);
                commonDataType = dataType;
            } else {
                if (dataType != commonDataType) {
                    throw new IllegalArgumentException(Resources.format(Resources.Keys.MismatchedDataType));
                }
                /*
                 * Get the domain of the combined image to create.
                 * TODO: current implementation computes the intersection of all sources.
                 * But a future version should allow users to specify if they want intersection,
                 * union or strict mode instead. A "strict" mode would prevent the combination of
                 * images using different domains (i.e. raise an error if domains are not the same).
                 */
                ImageUtilities.clipBounds(source, domain);
                if (domain.isEmpty()) {
                    throw new DisjointExtentException(Resources.format(Resources.Keys.SourceImagesDoNotIntersect));
                }
            }
        }
        if (domain == null) {
            // `domain` is guaranteed non-null if above block has been executed at least once.
            throw new IllegalArgumentException(Resources.format(Resources.Keys.UnspecifiedBands));
        }
        /*
         * Tile size is chosen after the domain has been computed, because we prefer a tile size which
         * is a divisor of the combined image size. Tile sizes of existing source images are preferred,
         * especially when the tiles are aligned, for increasing the chances that computing a tile of
         * the combined image causes the computation of a single tile of each source image.
         */
        long cx, cy;        // A combination of tile size with alignment on the tile matrix grid.
        cx = cy = (((long) Integer.MAX_VALUE) << Integer.SIZE) | ImageUtilities.DEFAULT_TILE_SIZE;
        final var tileGridXOffset = new FrequencySortedSet<Integer>(true);
        final var tileGridYOffset = new FrequencySortedSet<Integer>(true);
        for (final RenderedImage source : sources) {
            cx = chooseTileSize(cx, source.getTileWidth(),  domain.width,  domain.x - source.getMinX());
            cy = chooseTileSize(cy, source.getTileHeight(), domain.height, domain.y - source.getMinY());
            tileGridXOffset.add(source.getTileGridXOffset());
            tileGridYOffset.add(source.getTileGridYOffset());
        }
        final var preferredTileSize = new Dimension((int) cx, (int) cy);
        final boolean exactTileSize = ((cx | cy) >>> Integer.SIZE) == 0;
        allowSharing &= exactTileSize;
        return new MultiSourceLayout(sources, bandsPerSource, domain, preferredTileSize, exactTileSize,
                chooseMinTile(tileGridXOffset, domain.x, preferredTileSize.width),
                chooseMinTile(tileGridYOffset, domain.y, preferredTileSize.height),
                commonDataType, aggregate.numBands(), allowSharing ? scanlineStride : 0);
    }

    /**
     * Creates a new image layout from the values computed by {@code create(…)}.
     *
     * @param  sources            images to combine, in order.
     * @param  bandsPerSource     bands to use for each source image, in order. May contain {@code null} elements.
     * @param  domain             bounds of the image to create.
     * @param  preferredTileSize  the preferred tile size.
     * @param  commonDataType     data type of the combined image.
     * @param  scanlineStride     common scanline stride if data buffers will be shared, or 0 if no sharing.
     * @param  numBands           number of bands of the image to create.
     */
    private MultiSourceLayout(final RenderedImage[] sources, final int[][] bandsPerSource,
            final Rectangle domain, final Dimension preferredTileSize, final boolean exactTileSize,
            final int minTileX, final int minTileY, final int commonDataType, final int numBands,
            final int scanlineStride)
    {
        super(preferredTileSize, false);
        this.exactTileSize  = exactTileSize;
        this.bandsPerSource = bandsPerSource;
        this.sources        = sources;
        this.domain         = domain;
        this.minTileX       = minTileX;
        this.minTileY       = minTileY;
        this.sampleModel    = createBandedSampleModel(commonDataType, numBands, null, domain, scanlineStride);
        /*
         * Note: above call to `createBandedSampleModel(…)` must be last,
         * except for `filteredSources` which is not needed by that method.
         */
        filteredSources = new RenderedImage[sources.length];
        for (int i=0; i<filteredSources.length; i++) {
            RenderedImage source = sources[i];
            final int[] bands = bandsPerSource[i];
            if (bands != null) {
                source = BandSelectImage.create(source, bands);
            }
            filteredSources[i] = source;
        }
    }

    /**
     * Chooses the preferred tile size (width or height) between two alternatives.
     * The alternatives are {@code current} and {@code tile}. Note that having the
     * same tile size than source image is desirable but not mandatory.
     *
     * <p>The ideal situation is to have aligned tiles, so that computing one tile of combined image
     * will always cause the computation of only one tile of source image. For approaching that goal,
     * we pack the "grid alignment distance" ({@code offset}) in the higher bits and the tile size in
     * the lower bits of a {@code long}. Minimizing that {@code long} while optimize the offset first,
     * then choose the smallest tile size between tile matrices at the same offset.</p>
     *
     * @param  current    the current tile size, or 0 if none.
     * @param  tileSize   tile size (width or height) of the source image to consider.
     * @param  imageSize  full size (width or height) of the source image to consider.
     * @param  offset     location of the first pixel of the combined image relative to the first pixel of the source image.
     * @return the preferred tile size: {@code current} or {@code tile}.
     */
    private static long chooseTileSize(final long current, final int tileSize, final int imageSize, final int offset) {
        if ((imageSize % tileSize) == 0) {
            long c = Math.floorMod(offset, tileSize);   // How close the grid are aligned (ideal would be zero).
            c <<= Integer.SIZE;                         // Pack grid offset in higher bits.
            c |= tileSize;                              // Pack tile size in lower bits.
            /*
             * The use of `compareUnsigned(…)` below is an opportunistic trick for discarding negative tile size
             * (should be illegal, but we are paranoiac). If tile size was negative, the first bit should be set,
             * which is interpreted as a very high value when comparing as unsigned numbers.
             */
            if (Long.compareUnsigned(c, current) < 0) {
                return c;
            }
        }
        return current;
    }

    /**
     * Chooses the minimum tile index ({@code minTileX} or {@code minTileY}).
     * This value does not really matter, it has no incidence on performance or positional accuracy.
     * However using the same "tile grid offset" in the combined image compared to the source images
     * can make debugging easier. This is similar to using the same "grid to CRS" transform in grid
     * coverages.
     *
     * @param  offsets   all "tile grid offsets" sorted with most frequently used first.
     * @param  min       the minimal pixel coordinates in the combined image.
     * @param  tileSize  the tile size (width or height).
     * @return suggested minimum tile index.
     */
    private static int chooseMinTile(final FrequencySortedSet<Integer> offsets, final int min, final int tileSize) {
        for (int offset : offsets) {
            offset = min - offset;
            if (offset % tileSize == 0) {
                return offset / tileSize;
            }
        }
        return 0;
    }

    /**
     * Returns indices of the first tile ({@code minTileX}, {@code minTileY}).
     * Other tile matrix properties such as the number of tiles and the grid offsets will be derived
     * from those values together with the {@linkplain #domain}. Contrarily to pixel coordinates,
     * the tile coordinate space does not need to be the same for all images.
     *
     * @return indices of the first tile ({@code minTileX}, {@code minTileY}).
     */
    @Override
    public Point getMinTile() {
        return new Point(minTileX, minTileY);
    }

    /**
     * Suggests a tile size for the specified image size.
     * It may be exactly the size of the tiles of a source image, but not necessarily.
     * This method may compute a tile size different than the tile size of all sources.
     */
    @Override
    public Dimension suggestTileSize(int imageWidth, int imageHeight, boolean allowPartialTiles) {
        if (exactTileSize) return getPreferredTileSize();
        return super.suggestTileSize(imageWidth, imageHeight, allowPartialTiles);
    }

    /**
     * Suggests a tile size for operations derived from the given image.
     * It may be exactly the size of the tiles of a source image, but not necessarily.
     * This method may compute a tile size different than the tile size of all sources.
     */
    @Override
    public Dimension suggestTileSize(RenderedImage image, Rectangle bounds, boolean allowPartialTiles) {
        if (exactTileSize) return getPreferredTileSize();
        return super.suggestTileSize(image, bounds, allowPartialTiles);
    }

    /**
     * Returns {@code true} if all filtered sources are writable.
     *
     * @return whether a destination using all filtered sources could be writable.
     */
    final boolean isWritable() {
        for (final RenderedImage source : filteredSources) {
            if (!(source instanceof WritableRenderedImage)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds a default color model with the colors of the first visible band found in source images.
     * If a band is declared visible according {@link ImageUtilities#getVisibleBand(RenderedImage)},
     * then the returned color model will reuse the colors of that visible band.
     * Otherwise a grayscale color model is built with a value range inferred from the data-type.
     *
     * @param  colorizer  user-supplied provider of color model, or {@code null} if none.
     */
    final ColorModel createColorModel(final Colorizer colorizer) {
        ColorModel colors = null;
        int visibleBand = ColorModelFactory.DEFAULT_VISIBLE_BAND;
        int base = 0;
search: for (int i=0; i < sources.length; i++) {
            final RenderedImage source = sources[i];
            final int[] bands = bandsPerSource[i];
            final int vb = ImageUtilities.getVisibleBand(source);
            if (vb >= 0) {
                if (bands == null) {
                    visibleBand = base + vb;
                    colors = source.getColorModel();
                    break;
                }
                for (int j=0; j<bands.length; j++) {
                    if (bands[j] == vb) {
                        visibleBand = base + j;
                        colors = source.getColorModel();
                        break search;
                    }
                }
            }
            base += (bands != null) ? bands.length : ImageUtilities.getNumBands(source);
        }
        if (colorizer != null) {
            Optional<ColorModel> candidate = colorizer.apply(new Colorizer.Target(sampleModel, null, visibleBand));
            if (candidate.isPresent()) {
                return candidate.get();
            }
        }
        colors = ColorModelFactory.derive(colors, sampleModel.getNumBands(), visibleBand);
        if (colors != null) {
            return colors;
        }
        return ColorModelFactory.createGrayScale(sampleModel, visibleBand, null);
    }
}
