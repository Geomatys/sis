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
package org.apache.sis.gui.coverage;

import java.util.EnumMap;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import javafx.application.Platform;
import javafx.beans.DefaultProperty;
import javafx.scene.control.Control;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Separator;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Toggle;
import javafx.scene.layout.Region;
import javafx.event.ActionEvent;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import org.apache.sis.coverage.grid.GridCoverage;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.internal.gui.Resources;
import org.apache.sis.internal.gui.ToolbarButton;
import org.apache.sis.internal.gui.NonNullObjectProperty;
import org.apache.sis.util.ArgumentChecks;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.util.resources.Vocabulary;
import org.apache.sis.gui.referencing.RecentReferenceSystems;
import org.apache.sis.gui.map.StatusBar;
import org.apache.sis.gui.Widget;
import org.apache.sis.storage.Resource;


/**
 * An image or tabular view of {@link GridCoverage} together with controls for band selection and other operations.
 * The class contains two properties:
 *
 * <ul>
 *   <li>A {@link GridCoverage} supplied by user, or an {@link ImageRequest} for loading a coverage.</li>
 *   <li>A {@link View} type which specify how to show the coverage:
 *     <ul>
 *       <li>using {@link GridView} for showing numerical values in a table, or</li>
 *       <li>using {@link CoverageCanvas} for showing the coverage as an image.</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * Controls are provided for allowing user to customize map projection, number formats, <i>etc.</i>.
 * The set of control depends on the view type.
 *
 * <h2>Limitations</h2>
 * Current implementation is restricted to {@link GridCoverage} instances, but a future
 * implementation may generalize to {@link org.opengis.coverage.Coverage} instances.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 *
 * @see CoverageCanvas
 * @see GridView
 *
 * @since 1.1
 * @module
 */
@DefaultProperty("coverage")
public class CoverageExplorer extends Widget {
    /**
     * Type of view shown in the explorer.
     * It may be either an image or a table of numerical values.
     *
     * @see #viewTypeProperty
     */
    public enum View {
        // Enumeration order is the order in which buttons will appear on the button bar.

        /**
         * Shows the coverage visual as an image. This view uses {@link CoverageCanvas}.
         */
        IMAGE("\uD83D\uDDFA\uFE0F", Resources.Keys.TabularData),    // 🗺 — World map.

        /**
         * Shows the coverage numerical value in a table. This view uses {@link GridView}.
         */
        TABLE("\uD83D\uDD22\uFE0F", Resources.Keys.Visualize);      // 🔢 — Input symbol for numbers.

        /**
         * Number of enumeration values.
         */
        static final int COUNT = 2;

        /**
         * The Unicode characters to use as icon.
         */
        final String icon;

        /**
         * Key from {@link Resources} bundle for the localized text to use as tooltip.
         */
        final short tooltip;

        /**
         * Creates a new enumeration value.
         */
        private View(final String icon, short tooltip) {
            this.icon = icon;
            this.tooltip = tooltip;
        }
    }

    /**
     * The coverage shown in this explorer. Note that setting this property to a non-null value may not
     * modify the view content immediately. Instead, a background process will request the tiles.
     *
     * <p>Current implementation is restricted to {@link GridCoverage} instances, but a future
     * implementation may generalize to {@link org.opengis.coverage.Coverage} instances.</p>
     *
     * @see #getCoverage()
     * @see #setCoverage(GridCoverage)
     * @see #setCoverage(ImageRequest)
     */
    public final ObjectProperty<GridCoverage> coverageProperty;

    /**
     * The type of view (image or tabular data) shown in this explorer.
     *
     * @see #getViewType()
     * @see #setViewType(View)
     */
    public final ObjectProperty<View> viewTypeProperty;

    /**
     * Whether the {@link #coverageProperty} is in process of being set,
     * in which case some listeners should not react.
     */
    private boolean isCoverageAdjusting;

    /**
     * The control that put everything together, created when first requested.
     * The type of control may change in any future SIS version.
     *
     * @see #getView()
     */
    private SplitPane content;

    /**
     * The different views we can provide on {@link #coverageProperty}, together with associated controls.
     * Values in this map are initially null and created when first needed.
     * Concrete classes are {@link GridControls} and {@link CoverageControls}.
     *
     * @see #getControl(View)
     */
    private final EnumMap<View,ViewAndControls> views;

    /**
     * Handles the {@link javafx.scene.control.ChoiceBox} and menu items for selecting a CRS.
     */
    private final RecentReferenceSystems referenceSystems;

    /**
     * Creates an initially empty explorer with default view type.
     * By default {@code CoverageExplorer} will show a coverage as a table of values,
     * i.e. the default view type is {@link View#TABLE}.
     *
     * <div class="note"><b>API note:</b>
     * the reason for setting default value to tabular data is because it requires loading much less data with
     * {@link java.awt.image.RenderedImage}s supporting deferred tile loading. By contrast {@link View#IMAGE}
     * may require loading the full image.</div>
     */
    public CoverageExplorer() {
        this(View.TABLE);
    }

    /**
     * Creates an initially empty explorer with the specified view type.
     *
     * @param  type  the way to show coverages in this explorer.
     *
     * @see #setViewType(View)
     *
     * @since 1.2
     */
    public CoverageExplorer(final View type) {
        ArgumentChecks.ensureNonNull("type", type);
        coverageProperty = new SimpleObjectProperty<>(this, "coverage");
        viewTypeProperty = new NonNullObjectProperty<>(this, "viewType", type);
        coverageProperty.addListener((p,o,n) -> onCoverageSpecified(n));
        referenceSystems = new RecentReferenceSystems();
        referenceSystems.addUserPreferences();
        referenceSystems.addAlternatives("EPSG:4326", "EPSG:3395");         // WGS 84 / World Mercator
        /*
         * The coverage property may be shown in various ways (tabular data, image).
         * Each visualization way is a value in the `views` map.
         * Elements will be created when first needed.
         */
        views = new EnumMap<>(View.class);
    }

    /**
     * Returns the view-control pair for the given view type.
     * The view-control pair is created when first needed.
     */
    private ViewAndControls getViewAndControls(final View type) {
        ViewAndControls c = views.get(type);
        if (c == null) {
            final Vocabulary vocabulary = Vocabulary.getResources(getLocale());
            switch (type) {
                case TABLE: c = new GridControls(referenceSystems, vocabulary); break;
                case IMAGE: c = new CoverageControls(vocabulary, coverageProperty, referenceSystems); break;
                default: throw new AssertionError(type);
            }
            SplitPane.setResizableWithParent(c.controls(), Boolean.FALSE);
            SplitPane.setResizableWithParent(c.view(),     Boolean.TRUE);
            views.put(type, c);
        }
        return c;
    }

    /**
     * Returns the region containing the grid or coverage view, band selector and any control managed by this
     * {@code CoverageExplorer}. The {@link Region} subclass returned by this method is implementation dependent
     * and may change in any future version.
     *
     * @return the region to show.
     *
     * @see #getDataView(View)
     * @see #getControls(View)
     */
    @Override
    public final Region getView() {
        assert Platform.isFxApplicationThread();
        /*
         * We build when first requested because `ResourceExplorer` for example will never request this view.
         * Instead it will invoke `getDataView(View)` or `getControls(View)` and layout those regions itself.
         */
        if (content == null) {
            /*
             * Prepare buttons to add on the toolbar. Those buttons are not managed by this class;
             * they are managed by org.apache.sis.gui.dataset.DataWindow. We only declare here the
             * text and action for each button.
             */
            final ToggleGroup group   = new ToggleGroup();
            final Control[]   buttons = new Control[View.COUNT + 1];
            final Resources localized = Resources.forLocale(getLocale());
            buttons[0] = new Separator();
            for (final View type : View.values()) {
                buttons[1 + type.ordinal()] = new Selector(type).createButton(group, type.icon, localized, type.tooltip);
            }
            final View type = getViewType();
            final ViewAndControls c = getViewAndControls(type);
            group.selectToggle(group.getToggles().get(type.ordinal()));
            content = new SplitPane(c.controls(), c.view());
            ToolbarButton.insert(content, buttons);
            viewTypeProperty.addListener((p,o,n) -> onViewTypeSpecified(n));
            /*
             * The divider position is supposed to be a fraction between 0 and 1. A value of 1 would mean
             * to give all the space to controls and no space to data, which is not what we want. However
             * experience with JavaFX 14 shows that this setting gives just a reasonable space to controls
             * and most space to data. I have not identified the cause of this surprising behavior.
             * A smaller value results in too few space for the controls.
             */
            content.setDividerPosition(0, 1);
        }
        return content;
    }

    /**
     * Returns the region containing only the data visualization component, without controls.
     * This is a {@link GridView} or {@link CoverageCanvas} together with their {@link StatusBar}.
     * The {@link Region} subclass returned by this method is implementation dependent and may change
     * in any future version.
     *
     * @param  type  whether to obtain a {@link GridView} or {@link CoverageCanvas}.
     * @return the requested view for the {@link #coverageProperty}.
     */
    public final Region getDataView(final View type) {
        assert Platform.isFxApplicationThread();
        ArgumentChecks.ensureNonNull("type", type);
        return getViewAndControls(type).view();
    }

    /**
     * Returns the region containing only the controls, without data visualization component.
     * The {@link Region} subclass returned by this method is implementation dependent and may
     * change in any future version.
     *
     * @param  type  whether to obtain controls for {@link GridView} or {@link CoverageCanvas}.
     * @return the controls on specified data view.
     */
    public final Region getControls(final View type) {
        assert Platform.isFxApplicationThread();
        ArgumentChecks.ensureNonNull("type", type);
        return getViewAndControls(type).controls();
    }

    /**
     * The action to execute when the user selects a view.
     */
    private final class Selector extends ToolbarButton {
        /** The view to select when the button is pressed. */
        private final View type;

        /** Creates a new action which will show the view at the given index. */
        Selector(final View type) {
            this.type = type;
        }

        /** Invoked when the user selects another view to show (tabular data or the image). */
        @Override public void handle(final ActionEvent event) {
            final Toggle button = (Toggle) event.getSource();
            if (button.isSelected()) {
                setViewType(type);
                views.get(type).selector = button;    // Should never null null.
            } else {
                button.setSelected(true);       // Prevent situation where all buttons are unselected.
            }
        }
    }

    /**
     * Returns the source of sample values for this explorer.
     * This method, like all other methods in this class, shall be invoked from the JavaFX thread.
     *
     * @return the coverage shown in this explorer, or {@code null} if none.
     *
     * @see #coverageProperty
     */
    public final GridCoverage getCoverage() {
        return coverageProperty.get();
    }

    /**
     * Sets the coverage to show in this view.
     * This method shall be invoked from JavaFX thread and returns immediately.
     * The new data are loaded in a background thread and will appear after an
     * undetermined amount of time.
     *
     * @param  coverage  the data to show in this explorer, or {@code null} if none.
     *
     * @see #coverageProperty
     */
    public final void setCoverage(final GridCoverage coverage) {
        coverageProperty.set(coverage);
    }

    /**
     * Loads coverage in a background thread from the given source.
     * This method shall be invoked from JavaFX thread and returns immediately.
     * The grid content may appear unmodified after this method returns;
     * the modifications will appear after an undetermined amount of time.
     *
     * @param  source  the coverage or resource to load, or {@code null} if none.
     */
    public final void setCoverage(final ImageRequest source) {
        assert Platform.isFxApplicationThread();
        if (source == null) {
            setCoverage((GridCoverage) null);
        } else if (source.listener != null) {
            throw new IllegalArgumentException(Errors.getResources(getLocale())
                    .getString(Errors.Keys.AlreadyInitialized_1, "listener"));
        } else {
            source.listener = this;
            startLoading(source);
        }
    }

    /**
     * Invoked when a new coverage has been set on the {@link #coverageProperty}.
     * This method notifies the GUI controls about the change then starts loading
     * data in a background thread.
     *
     * @param  coverage  the new coverage.
     */
    private void onCoverageSpecified(final GridCoverage coverage) {
        if (!isCoverageAdjusting) {
            startLoading(null);                                         // Clear data.
            notifyCoverageChange(coverage, null);
            if (coverage != null) {
                startLoading(new ImageRequest(coverage, null));         // Start a background thread.
            }
        }
    }

    /**
     * Invoked in JavaFX thread by {@link GridView} after the coverage has been read.
     *
     * @param  coverage    the new coverage, or {@code null} if loading failed or has been cancelled.
     * @param  originator  resource from which the data has been read, or {@code null} if unknown.
     */
    final void onCoverageLoaded(final GridCoverage coverage, final Resource originator) {
        notifyCoverageChange(coverage, (originator != null) ? new WeakReference<>(originator) : null);
        isCoverageAdjusting = true;
        try {
            setCoverage(coverage);
        } finally {
            isCoverageAdjusting = false;
        }
    }

    /**
     * Invoked by {@link #setCoverage(ImageRequest)} for starting data loading in a background thread.
     * This method is invoked in JavaFX thread.
     *
     * @param  source  the coverage or resource to load, or {@code null} if none.
     */
    private void startLoading(final ImageRequest source) {
        final GridView main = (GridView) getViewAndControls(View.TABLE).view();
        main.setImage(source);
    }

    /**
     * Invoked in JavaFX thread after {@link #setCoverage(ImageRequest)} completion for notifying controls
     * about the coverage change. Controls should update the GUI with new information available,
     * in particular the coordinate reference system and the list of sample dimensions.
     *
     * @param  data        the new coverage, or {@code null} if none.
     * @param  originator  the resource from which the data has been read, or {@code null} if unknown.
     */
    private void notifyCoverageChange(final GridCoverage data, final Reference<Resource> originator) {
        if (data != null) {
            final GridGeometry gg = data.getGridGeometry();
            referenceSystems.areaOfInterest.set(gg.isDefined(GridGeometry.ENVELOPE) ? gg.getEnvelope() : null);
            if (gg.isDefined(GridGeometry.CRS)) {
                referenceSystems.setPreferred(true, gg.getCoordinateReferenceSystem());
            }
        }
        for (final ViewAndControls c : views.values()) {
            c.coverageChanged(data, originator);
        }
    }

    /**
     * Returns the type of view (image or tabular data) shown in this explorer.
     * The default value is {@link View#TABLE}.
     *
     * @return the way to show coverages in this explorer.
     *
     * @see #viewTypeProperty
     */
    public final View getViewType() {
        return viewTypeProperty.get();
    }

    /**
     * Sets the type of view to show in this explorer.
     *
     * @param  type  the new way to show coverages in this explorer.
     *
     * @see #viewTypeProperty
     */
    public final void setViewType(final View type) {
        viewTypeProperty.set(type);
    }

    /**
     * Invoked when a new view type has been specified.
     *
     * @param  type  the new way to show coverages in this explorer.
     */
    private void onViewTypeSpecified(final View type) {
        final ViewAndControls c = getViewAndControls(type);
        content.getItems().setAll(c.controls(), c.view());
        final Toggle selector = c.selector;
        if (selector != null) {
            selector.setSelected(true);
        }
    }
}
