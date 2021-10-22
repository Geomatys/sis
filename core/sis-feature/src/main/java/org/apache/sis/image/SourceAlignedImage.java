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

import java.util.Set;
import java.awt.image.ColorModel;
import java.awt.image.SampleModel;
import java.awt.image.RenderedImage;
import org.apache.sis.util.ArraysExt;
import org.apache.sis.util.Workaround;
import org.apache.sis.internal.jdk9.JDK9;


/**
 * An image computed from a single source and sharing the same coordinate system.
 * In addition of pixel coordinate system, images share also the same tile indices.
 * Tiles in this image have the same size than tiles in the source image.
 *
 * <div class="note"><b>Relationship with other classes</b><br>
 * This class is similar to {@link ImageAdapter} except that it extends {@link ComputedImage}
 * and does not forward {@link #getTile(int, int)}, {@link #getData()} and other data methods
 * to the source image.</div>
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 * @since   1.1
 * @module
 */
abstract class SourceAlignedImage extends ComputedImage {
    /**
     * Convenience collection for subclasses that inherit all properties related to positioning.
     * May be used as the {@code inherit} argument in {@link #filterPropertyNames(String[], Set, String[])}.
     * Inheriting those properties make sense for operations that do not change pixel coordinates.
     */
    static final Set<String> POSITIONAL_PROPERTIES = JDK9.setOf(GRID_GEOMETRY_KEY,
            POSITIONAL_ACCURACY_KEY, ResampledImage.POSITIONAL_CONSISTENCY_KEY);

    /**
     * The color model for this image.
     */
    private final ColorModel colorModel;

    /**
     * Creates a new image with the given source.
     * This image inherit the color model and sample model of source image.
     *
     * @param  source  the image to use as a background for this image.
     */
    protected SourceAlignedImage(final RenderedImage source) {
        super(source.getSampleModel(), source);
        colorModel = source.getColorModel();
    }

    /**
     * Creates a new image with the given source, color model and sample model.
     *
     * @param  source       source of this image. Shall not be null.
     * @param  colorModel   the color model of the new image.
     * @param  sampleModel  the sample model of the new image.
     */
    SourceAlignedImage(final RenderedImage source, final ColorModel colorModel, final SampleModel sampleModel) {
        super(sampleModel, source);
        this.colorModel = colorModel;
        assert source.getSampleModel().getWidth()  == sampleModel.getWidth() &&
               source.getSampleModel().getHeight() == sampleModel.getHeight();
    }

    /**
     * Creates a new image with the given source and a sample model derived from the given color model.
     * The new image will have the same tile size than the given image.
     *
     * @param  source       source of this image. Shall not be null.
     * @param  colorModel   the color model of the new image.
     */
    protected SourceAlignedImage(final RenderedImage source, final ColorModel colorModel) {
        super(createSampleModel(colorModel, source.getSampleModel()), source);
        this.colorModel = colorModel;
    }

    /**
     * Creates the sample model. This is a workaround for RFE #4093999
     * ("Relax constraint on placement of this()/super() call in constructors").
     */
    @Workaround(library="JDK", version="1.8")
    private static SampleModel createSampleModel(final ColorModel colorModel, final SampleModel original) {
        final SampleModel sm = colorModel.createCompatibleSampleModel(original.getWidth(), original.getHeight());
        return original.equals(sm) ? original : sm;
    }

    /**
     * Returns the color model associated with this image.
     *
     * @return the color model, or {@code null} if none.
     */
    @Override
    public final ColorModel getColorModel() {
        return colorModel;
    }

    /**
     * Returns the names of properties as a merge between source properties (after filtering) and properties added
     * by the subclass. This is a helper method for {@link #getPropertyNames()} implementations in subclasses.
     *
     * <p>The {@code names} argument should be the result of invoking {@link RenderedImage#getPropertyNames()}
     * on the source image. This method modifies directly that array returned by {@code getPropertyNames()} on
     * the assumption that the array is already a copy. This assumption is okay when the source is known to be
     * an Apache SIS implementation.</p>
     *
     * @param  names    names of properties to filter, or {@code null} if none.
     *                  If non-null, this array will be modified in-place.
     * @param  inherit  properties to inherit from the source.
     * @param  append   properties to append, or {@code null} if none.
     * @return properties recognized by this image, or {@code null} if none.
     */
    static String[] filterPropertyNames(String[] names, final Set<String> inherit, final String[] append) {
        if (names == null) {
            return (append != null) ? append.clone() : null;
        }
        int n = 0;
        for (final String name : names) {
            if (inherit.contains(name)) {
                names[n++] = name;
            }
        }
        if (append == null) {
            return ArraysExt.resize(names, n);
        }
        names = ArraysExt.resize(names, n + append.length);
        System.arraycopy(names, n, append, 0, append.length);
        return names;
    }

    /**
     * Delegates to source image.
     */
    @Override public final int getMinX()            {return getSource().getMinX();}
    @Override public final int getMinY()            {return getSource().getMinY();}
    @Override public final int getWidth()           {return getSource().getWidth();}
    @Override public final int getHeight()          {return getSource().getHeight();}
    @Override public final int getMinTileX()        {return getSource().getMinTileX();}
    @Override public final int getMinTileY()        {return getSource().getMinTileY();}
    @Override public final int getNumXTiles()       {return getSource().getNumXTiles();}
    @Override public final int getNumYTiles()       {return getSource().getNumYTiles();}
    @Override public final int getTileWidth()       {return getSource().getTileWidth();}
    @Override public final int getTileHeight()      {return getSource().getTileHeight();}
    @Override public final int getTileGridXOffset() {return getSource().getTileGridXOffset();}
    @Override public final int getTileGridYOffset() {return getSource().getTileGridYOffset();}
}
