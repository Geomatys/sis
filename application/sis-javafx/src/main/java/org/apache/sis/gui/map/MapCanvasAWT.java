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
package org.apache.sis.gui.map;

import java.util.Locale;
import java.nio.IntBuffer;
import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.GraphicsEnvironment;
import java.awt.GraphicsConfiguration;
import java.awt.image.DataBufferInt;
import java.awt.image.RenderedImage;
import java.awt.image.BufferedImage;
import java.awt.image.VolatileImage;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Rectangle2D;
import javafx.scene.image.ImageView;
import javafx.scene.image.PixelBuffer;
import javafx.scene.image.PixelFormat;
import javafx.scene.image.WritableImage;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.concurrent.Task;
import javafx.util.Callback;
import org.apache.sis.internal.coverage.j2d.ColorModelFactory;


/**
 * A canvas for maps to be rendered using Java2D from Abstract Window Toolkit.
 * The map is rendered using Java2D in a background thread, then copied in a JavaFX image.
 * Java2D is used for rendering the map because it may contain too many elements for a scene graph.
 * After the map has been rendered, other JavaFX nodes can be put on top of the map, typically for
 * controls by the user.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public abstract class MapCanvasAWT extends MapCanvas {
    /**
     * Whether to try to get native acceleration in the {@link VolatileImage} used for painting the map.
     * Native acceleration is of limited interested here because even if painting occurs in video card
     * memory, it is copied to Java heap before to be transferred to JavaFX image, which may itself copy
     * back to video card memory. I'm not aware of a way to perform direct transfer from AWT to JavaFX.
     * Consequently before to enable this acceleration, we should benchmark to see if it is worth.
     */
    private static final boolean NATIVE_ACCELERATION = false;

    /**
     * Number of additional pixels to paint on each sides of the image, outside the viewing area.
     * Computing a larger image reduces the black borders that user sees during translations or
     * during zoom out before the new image is repainted. The default value is {@code null}.
     */
    public final ObjectProperty<Insets> imageMargin;

    /**
     * A buffer where to draw the content of the map for the region to be displayed.
     * This buffer uses ARGB color model, contrarily to the {@link RenderedImage} of
     * {@link org.apache.sis.coverage.grid.GridCoverage} which may have any color model.
     * This buffered image will contain only the visible region of the map;
     * it may be a zoom over a small region.
     *
     * <p>This buffered image contains the same data than the {@linkplain #image} of this canvas.
     * Those two images will share the same data array (no copy) and the same coordinate system.</p>
     *
     * <h4>Restriction</h4>
     * Type is restricted to {@link BufferedImage#TYPE_INT_ARGB_PRE} or {@link BufferedImage#TYPE_4BYTE_ABGR_PRE}
     * because JavaFX {@link PixelBuffer} (stored in {@link #bufferWrapper}) accepts only those types.
     * We arbitrarily choose {@code TYPE_INT_ARGB_PRE}.
     */
    private BufferedImage buffer;

    /**
     * A temporary buffer where to draw the {@link RenderedImage} in a background thread.
     * We use this double-buffering when the {@link #buffer} is already wrapped by JavaFX.
     * After creating the image in background, its content is copied to {@link #buffer} in
     * JavaFX thread.
     */
    private VolatileImage doubleBuffer;

    /**
     * The graphic configuration at the time {@link #buffer} has been rendered.
     * Used for creating compatible {@link #doubleBuffer} before updating image content.
     * This configuration determines whether native acceleration will be enabled or not.
     *
     * @see #NATIVE_ACCELERATION
     */
    private GraphicsConfiguration bufferConfiguration;

    /**
     * Wraps {@link #buffer} data array for use by JavaFX images. This is the mechanism used
     * by JavaFX 13+ for allowing {@link #image} to share the same data than {@link #buffer}.
     * The same wrapper can be used for many {@link WritableImage} instances (e.g. thumbnails).
     *
     * <h4>Invariants</h4>
     * <ul>
     *   <li>Shall be non-null if and only If {@link #buffer} is non-null.</li>
     * </ul>
     */
    private PixelBuffer<IntBuffer> bufferWrapper;

    /**
     * The node where the rendered map will be shown. Its content is prepared in a background thread
     * by {@link Renderer}. Subclasses should not set the image content directly.
     */
    protected final ImageView image;

    /**
     * Creates a new canvas for JavaFX application.
     *
     * @param  locale  the locale to use for labels and some messages, or {@code null} for default.
     */
    public MapCanvasAWT(final Locale locale) {
        super(locale);
        imageMargin = new SimpleObjectProperty<>(this, "imageMargin");
        image = new ImageView();
        image.setPreserveRatio(true);
        floatingPane.getChildren().add(image);
    }

    /**
     * Clears {@link #buffer} and all support fields.
     */
    private void clearBuffer() {
        buffer              = null;
        doubleBuffer        = null;
        bufferWrapper       = null;
        bufferConfiguration = null;
    }

    /**
     * Invoked in JavaFX thread for creating a renderer to be executed in a background thread.
     * Subclasses should copy in this method all {@code MapCanvas} properties that the background thread
     * will need for performing the rendering process.
     *
     * @return rendering process to be executed in background thread,
     *         or {@code null} if there is nothing to paint.
     */
    @Override
    protected abstract Renderer createRenderer();

    /**
     * A snapshot of {@link MapCanvasAWT} state to paint as an image.
     * The snapshot is created in JavaFX thread by the {@link MapCanvasAWT#createRenderer()} method,
     * then the rendering process is executed in a background thread.
     * Methods are invoked in the following order:
     *
     * <table class="sis">
     *   <caption>Methods invoked during a map rendering process</caption>
     *   <tr><th>Method</th>                     <th>Thread</th>            <th>Remarks</th></tr>
     *   <tr><td>{@link #createRenderer()}</td>  <td>JavaFX thread</td>     <td>Collects all needed information.</td></tr>
     *   <tr><td>{@link #render()}</td>          <td>Background thread</td> <td>Computes what can be done in advance.</td></tr>
     *   <tr><td>{@link #paint(Graphics2D)}</td> <td>Background thread</td> <td>Holds a {@link Graphics2D}.</td></tr>
     *   <tr><td>{@link #commit(MapCanvas)}</td> <td>JavaFX thread</td>     <td>Saves data to cache for reuse.</td></tr>
     * </table>
     *
     * This class should not access any {@link MapCanvasAWT} property from a method invoked in background thread
     * ({@link #render()} and {@link #paint(Graphics2D)}). It may access {@link MapCanvasAWT} properties from the
     * {@link #commit(MapCanvas)} method.
     *
     * @author  Martin Desruisseaux (Geomatys)
     * @version 1.1
     * @since   1.1
     * @module
     */
    protected abstract static class Renderer extends MapCanvas.Renderer {
        /**
         * Values of the {@link MapCanvasAWT#imageMargin} property at construction time.
         * Those values are initialized by {@link #isValid(Insets, BufferedImage)}.
         */
        private int left, top;

        /**
         * Image width and height, taking in account the margins.
         * Those values are initialized by {@link #isValid(Insets, BufferedImage)}.
         */
        private int width, height;

        /**
         * Creates a new renderer. The {@linkplain #getWidth() width} and {@linkplain #getHeight() height}
         * are initially zero; they will get a non-zero values before {@link #paint(Graphics2D)} is invoked.
         */
        protected Renderer() {
        }

        /**
         * Rounds and clamp the given value. The upper limit is arbitrary.
         */
        private static int clamp(final double value) {
            return (int) Math.max(0, Math.min(Short.MAX_VALUE, Math.round(value)));
        }

        /**
         * Returns whether the given buffer is non-null and has the expected size.
         * This verification shall be done only after {@link #initialize(Pane)} has been invoked.
         *
         * @param  margin  value of {@link #imageMargin}.
         * @param  buffer  value of {@link #buffer}.
         */
        private boolean isValid(final Insets margin, final BufferedImage buffer) {
            width  = getWidth();
            height = getHeight();
            if (margin != null) {
                final int right, bottom;
                top    = clamp(margin.getTop());
                right  = clamp(margin.getRight());
                bottom = clamp(margin.getBottom());
                left   = clamp(margin.getLeft());
                width  = Math.addExact(width, left + right);
                height = Math.addExact(height, top + bottom);
            }
            return (buffer != null)
                    && buffer.getWidth()  == width
                    && buffer.getHeight() == height;
        }

        /**
         * Applies translation on the given graphics before {@link #paint(Graphics2D)}.
         */
        private void translate(final Graphics2D gr) {
            gr.translate(left, top);
        }

        /**
         * Compensates the translation applied by {@link #translate(Graphics2D)}.
         * This method is invoked only if the image painting has been successful,
         * otherwise we assume that old content is still present and require the
         * old translations.
         */
        private void translate(final ImageView image) {
            image.setTranslateX(-left);
            image.setTranslateY(-top);
        }

        /**
         * Invoked in a background thread before {@link #paint(Graphics2D)}. Subclasses can override
         * this method if some rendering steps do not need {@link Graphics2D} handler. Doing work in
         * advance allow to hold the {@link Graphics2D} handler for a shorter time.
         *
         * <p>The default implementation does nothing.</p>
         *
         * @throws Exception if an error occurred while preparing data.
         */
        @Override
        protected void render() throws Exception {
        }

        /**
         * Invoked after {@link #render()} for doing the actual map painting.
         * This method is invoked in a background thread, potentially many times if {@link VolatileImage} content
         * is invalidated in the middle of rendering process. This method should not access any {@link MapCanvas}
         * property; if some canvas properties are needed, they should have been copied at construction time.
         *
         * @param  gr  the Java2D handler to use for rendering the map.
         */
        protected abstract void paint(Graphics2D gr);

        /**
         * Invoked in JavaFX thread after successful {@link #paint(Graphics2D)} completion. This method can update the
         * {@link #floatingPane} children with the nodes (images, shaped, <i>etc.</i>) created by {@link #render()}.
         * If this method detects that data has changed during the time {@code Renderer} was working in background,
         * then this method can return {@code true} for requesting a new repaint. In such case that repaint will use
         * a new {@link Renderer} instance; the current instance will not be reused.
         *
         * <p>The default implementation does nothing and returns {@code true}.</p>
         *
         * @param  canvas  the canvas where drawing has been done. It will be a {@link MapCanvasAWT} instance.
         * @return {@code true} on success, or {@code false} if the rendering should be redone
         *         (for example because a change has been detected in the data).
         */
        @Override
        protected boolean commit(MapCanvas canvas) {
            return true;
        }
    }

    /**
     * Invoked when the map content needs to be rendered again into the {@link #image}.
     * It may be because the map has new content, or because the viewed region moved or
     * has been zoomed.
     *
     * <p>There is two possible situations:</p>
     * <ul class="verbose">
     *   <li>If the current buffers are not suitable, then we clear everything related to Java2D buffered images.
     *     Those resources will be recreated from scratch in background thread. There is no need for double-buffering
     *     in such case because the new {@link BufferedImage} will not be shared with JavaFX image before the end
     *     of this task.</li>
     *   <li>Otherwise (current buffer it still valid), we should not update {@link BufferedImage} in a background
     *     thread because the internal array of that image is shared with JavaFX image. That image can be updated
     *     only in JavaFX thread through the {@code PixelBuffer.update(…)} method. A {@link VolatileImage} is used
     *     as a temporary buffer.</li>
     * </ul>
     *
     * In all cases we need to be careful to not use directly any {@link MapCanvas} field from the {@code call()}
     * methods. Information needed by {@code call()} must be copied first.
     *
     * @see #requestRepaint()
     */
    @Override
    final Task<?> createWorker(final MapCanvas.Renderer mc) {
        assert Platform.isFxApplicationThread();
        final Renderer context = (Renderer) mc;
        if (!context.isValid(imageMargin.get(), buffer)) {
            clearBuffer();
            return new Creator(context);
        } else {
            return new Updater(context);
        }
    }

    /**
     * Background tasks for creating a new {@link BufferedImage}. This task is invoked when there is no
     * previous resources that we can recycle, either because they have never been created yet or because
     * they are not suitable anymore (for example because the image size changed).
     */
    private final class Creator extends Task<WritableImage> {
        /**
         * The user-provided object which will perform the actual rendering.
         * Its {@link Renderer#paint(Graphics2D)} method will be invoked in background thread.
         */
        private final Renderer renderer;

        /**
         * The Java2D image where to do the rendering. This image will be created in a background thread
         * and assigned to the {@link MapCanvasAWT#buffer} field in JavaFX thread if rendering succeed.
         */
        private BufferedImage drawTo;

        /**
         * Wrapper around {@link #buffer} internal array for interoperability between Java2D and JavaFX.
         * Created only if {@link #drawTo} have been successfully painted.
         */
        private PixelBuffer<IntBuffer> wrapper;

        /**
         * The graphic configuration at the time {@link #drawTo} has been rendered.
         * This will be used for creating {@link VolatileImage} when updating the image.
         */
        private GraphicsConfiguration configuration;

        /**
         * Creates a new task for painting without resource recycling.
         */
        Creator(final Renderer context) {
            renderer = context;
        }

        /**
         * Invoked in background thread for creating and rendering the image (may be slow).
         * Any {@link MapCanvas} property needed by this method shall be copied before the
         * background thread is executed; no direct reference to {@link MapCanvas} here.
         */
        @Override
        protected WritableImage call() throws Exception {
            renderer.render();
            final int width  = renderer.width;
            final int height = renderer.height;
            drawTo = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB_PRE);
            final Graphics2D gr = drawTo.createGraphics();
            try {
                configuration = gr.getDeviceConfiguration();
                renderer.translate(gr);
                renderer.paint(gr);
            } finally {
                gr.dispose();
            }
            if (NATIVE_ACCELERATION) {
                if (!configuration.getImageCapabilities().isAccelerated()) {
                    configuration = GraphicsEnvironment.getLocalGraphicsEnvironment()
                                    .getDefaultScreenDevice().getDefaultConfiguration();
                }
            }
            /*
             * The call to `array.getData()` below should be after we finished drawing in the new
             * BufferedImage, because this direct access to data array disables GPU accelerations.
             */
            final DataBufferInt array = (DataBufferInt) drawTo.getRaster().getDataBuffer();
            IntBuffer ib = IntBuffer.wrap(array.getData(), array.getOffset(), array.getSize());
            wrapper = new PixelBuffer<>(width, height, ib, PixelFormat.getIntArgbPreInstance());
            return new WritableImage(wrapper);
        }

        /**
         * Invoked in JavaFX thread on success. The JavaFX image is set to the result, then intermediate
         * buffers created by this task are saved in {@link MapCanvas} fields for reuse next time that
         * an image of the same size will be rendered again.
         */
        @Override
        protected void succeeded() {
            image.setImage(getValue());
            renderer.translate(image);
            buffer              = drawTo;
            bufferWrapper       = wrapper;
            bufferConfiguration = configuration;
            final boolean done  = renderer.commit(MapCanvasAWT.this);
            renderingCompleted(this);
            if (!done || contentsChanged()) {
                repaint();
            }
        }

        @Override protected void failed()    {renderingCompleted(this);}
        @Override protected void cancelled() {renderingCompleted(this);}
    }

    /**
     * Background tasks for painting in an existing {@link BufferedImage}. This task is invoked
     * when previous resources (JavaFX image and Java2D volatile/buffered image) can be reused.
     * The Java2D volatile image will be rendered in background thread, then its content will be
     * transferred to JavaFX image (through {@link BufferedImage} shared array) in JavaFX thread.
     */
    private final class Updater extends Task<VolatileImage> implements Callback<PixelBuffer<IntBuffer>, Rectangle2D> {
        /**
         * The user-provided object which will perform the actual rendering.
         * Its {@link Renderer#paint(Graphics2D)} method will be invoked in background thread.
         */
        private final Renderer renderer;

        /**
         * The buffer during last paint operation. This buffer will be reused if possible,
         * but may become invalid and in need to be recreated. May be {@code null}.
         */
        private VolatileImage previousBuffer;

        /**
         * The configuration to use for creating a new {@link VolatileImage}
         * if {@link #previousBuffer} is invalid.
         */
        private final GraphicsConfiguration configuration;

        /**
         * Whether {@link VolatileImage} content became invalid and needs to be recreated.
         */
        private boolean contentsLost;

        /**
         * Creates a new task for painting with resource recycling.
         */
        Updater(final Renderer context) {
            renderer       = context;
            previousBuffer = doubleBuffer;
            configuration  = bufferConfiguration;
        }

        /**
         * Invoked in background thread for rendering the image (may be slow).
         * Any {@link MapCanvas} field needed by this method shall be copied before the
         * background thread is executed; no direct reference to {@link MapCanvas} here.
         */
        @Override
        protected VolatileImage call() throws Exception {
            renderer.render();
            final int width  = renderer.width;
            final int height = renderer.height;
            VolatileImage drawTo = previousBuffer;
            previousBuffer = null;                      // For letting GC do its work.
            if (drawTo == null) {
                drawTo = configuration.createCompatibleVolatileImage(width, height, VolatileImage.TRANSLUCENT);
            }
            boolean invalid = true;
            try {
                do {
                    if (drawTo.validate(configuration) == VolatileImage.IMAGE_INCOMPATIBLE) {
                        drawTo = configuration.createCompatibleVolatileImage(width, height, VolatileImage.TRANSLUCENT);
                    }
                    final Graphics2D gr = drawTo.createGraphics();
                    try {
                        gr.setBackground(ColorModelFactory.TRANSPARENT);
                        gr.clearRect(0, 0, drawTo.getWidth(), drawTo.getHeight());
                        renderer.translate(gr);
                        renderer.paint(gr);
                    } finally {
                        gr.dispose();
                    }
                    invalid = drawTo.contentsLost();
                } while (invalid && !isCancelled());
            } finally {
                if (invalid) {
                    drawTo.flush();         // Release native resources.
                }
            }
            return drawTo;
        }

        /**
         * Invoked by {@link PixelBuffer#updateBuffer(Callback)} for updating the {@link #buffer} content.
         * This method must be invoked in JavaFX thread. It copies the {@link VolatileImage} content to the
         * {@link BufferedImage} shared with JavaFX in a single {@code Graphics2D.drawImage(…)} operation.
         * The whole destination surface shall be written by {@code drawImage(…)}, so there is no need to invoke
         * {@link Graphics2D#clearRect} first. It is important because the small delay between {@code clearRect(…)}
         * and {@code drawImage(…)} can cause twinkle.
         */
        @Override
        public Rectangle2D call(final PixelBuffer<IntBuffer> wrapper) {
            final VolatileImage drawTo = doubleBuffer;
            final Graphics2D gr = buffer.createGraphics();
            try {
                gr.setComposite(AlphaComposite.Src);        // Copy source (previous destination is discarded).
                gr.drawImage(drawTo, 0, 0, null);
                contentsLost = drawTo.contentsLost();
            } finally {
                gr.dispose();
            }
            return null;
        }

        /**
         * Invoked in JavaFX thread on success. The JavaFX image is set to the result, then the double buffer
         * created by this task is saved in {@link MapCanvas} fields for reuse next time that an image of the
         * same size will be rendered again.
         */
        @Override
        protected void succeeded() {
            final VolatileImage drawTo = getValue();
            doubleBuffer = drawTo;
            try {
                bufferWrapper.updateBuffer(this);   // This will invoke the `call(PixelBuffer)` method above.
            } finally {
                drawTo.flush();                     // Release native resources.
            }
            renderer.translate(image);
            final boolean done = renderer.commit(MapCanvasAWT.this);
            renderingCompleted(this);
            if (!done || contentsLost || contentsChanged()) {
                repaint();
            }
        }

        @Override protected void failed()    {renderingCompleted(this);}
        @Override protected void cancelled() {renderingCompleted(this);}
    }

    /**
     * Clears the image and all intermediate buffer.
     * Invoking this method may help to release memory when the map is no longer shown.
     */
    @Override
    protected void clear() {
        image.setImage(null);
        clearBuffer();
        super.clear();
    }
}
