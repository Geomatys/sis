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
package org.apache.sis.internal.gui.control;

import java.util.Objects;
import java.text.NumberFormat;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.ObservableList;
import javafx.scene.paint.Color;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.control.TableView;
import javafx.scene.control.TableColumn;
import javafx.scene.control.cell.CheckBoxTableCell;
import org.apache.sis.internal.gui.Styles;
import org.apache.sis.internal.util.Numerics;
import org.apache.sis.util.resources.Vocabulary;


/**
 * Provides a widget for associating numeric values to solid colors.
 * It can be used as a table of isolines or as a {@link ColorRamp} editor.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public final class ValueColorMapper extends ColorColumnHandler<ValueColorMapper.Step> {
    /**
     * Colors to associate to a given value.
     *
     * <h2>Ordering</h2>
     * {@code Step} natural ordering is inconsistent with equals.
     * Natural ordering is based on the {@linkplain #value} only,
     * while the {@link #equals(Object)} method compares all properties.
     */
    public static final class Step implements Comparable<Step> {
        /**
         * The value for which to associate a color. The initial value is {@link Double#NaN},
         * but that value should be used only for the insertion row.
         */
        public final DoubleProperty value;

        /**
         * Color associated to the {@linkplain #value}.
         *
         * The value type is {@link ColorRamp} for now. But if this property become public (i.e. located
         * in a non-internal package) in a future version then the type should be changed to {@link Color}
         * and bidirectionally binded to another property (package-private) of type {@link ColorRamp}.
         */
        public final ObjectProperty<ColorRamp> color;

        /**
         * Whether this step should be used. For example if {@code ValueColorMapper} is used as an isoline table,
         * then this property determines whether the isoline should be drawn on the map.
         */
        public final BooleanProperty visible;

        /**
         * Creates an empty step to be used as a placeholder for the insertion row.
         */
        Step() {
            value   = new SimpleDoubleProperty  (this, "value", Double.NaN);
            color   = new SimpleObjectProperty<>(this, "color");
            visible = new SimpleBooleanProperty (this, "visible");
        }

        /**
         * Creates a step associating the given color to the given value.
         */
        Step(final double value, final Color color) {
            this();
            this.value.set(value);
            this.color.set(new ColorRamp(color));
            visible.set(true);
        }

        /**
         * Compares this step value with the given value for order.
         * The comparison is applied only on the {@linkplain #value}.
         * The color and visibility state are ignored.
         *
         * @param  other  the other value to compare with this step.
         * @return +1 if the value of this step is higher than value of given step, -1 if smaller or 0 if equal.
         */
        @Override
        public int compareTo(final Step other) {
            return Double.compare(value.get(), other.value.get());
        }

        /**
         * Compares the given object with this value for equality.
         * This method compares all properties, including visibility and color.
         *
         * @param  other  the other object to compare with this step.
         * @return whether the other object is equals to this step.
         */
        @Override
        public boolean equals(final Object other) {
            if (other instanceof Step) {
                final Step that = (Step) other;
                return Numerics.equals(value.get(), that.value.get()) &&
                        Objects.equals(color.get(), that.color.get()) &&
                        visible.get() == that.visible.get();
            }
            return false;
        }

        /**
         * Returns a hash code value for this step.
         *
         * @return a hash code value for this step.
         */
        @Override
        public int hashCode() {
            return Double.hashCode(value.get()) + Objects.hashCode(color.get()) + Boolean.hashCode(visible.get());
        }

        /**
         * Returns a string representation of this step for debugging purposes.
         *
         * @return a string representation of this step.
         */
        @Override
        public String toString() {
            return Double.toString(value.get()) + " = " + Objects.toString(color.get());
        }
    }

    /**
     * The format to use for formatting numerical values.
     * The same instance will be shared by all {@link FormatTableCell}s in this table.
     */
    private final NumberFormat format;

    /**
     * Creates a new handler.
     */
    public ValueColorMapper() {
        format = NumberFormat.getInstance();
    }

    /**
     * Returns the colors to apply for the given step, or {@code null} for transparent.
     * This method is defined for safety but should not be invoked; use {@link #getObservableValue(S)} instead.
     *
     * @param  level  the value for which to get the color to show in color cell.
     */
    @Override
    protected int[] getARGB(final Step level) {
        final ColorRamp r = level.color.get();
        return (r != null) ? r.colors : null;
    }

    /**
     * Returns the color associated to given row as an observable value.
     *
     * @param  level  the value for which to get the color to show in color cell.
     * @return the color(s) to use for the given value, or {@code null} if none (transparent).
     */
    @Override
    protected ObservableValue<ColorRamp> getObservableValue(final Step level) {
        return level.color;
    }

    /**
     * Invoked when users confirmed that (s)he wants to use the selected colors.
     *
     * @param  level   the value for which to assign new color(s).
     * @param  colors  the new color for the given value, or {@code null} for resetting default value.
     * @return the type of color (always solid for this class).
     */
    @Override
    protected ColorRamp.Type applyColors(final Step level, ColorRamp colors) {
        level.color.set(colors);
        return ColorRamp.Type.SOLID;
    }

    /**
     * Invoked when a numerical value has been edited. This method saves the value,
     * checks the step as visible and set a default color if no color was already set.
     */
    private static void commitEdit(final TableColumn.CellEditEvent<Step,Number> event) {
        final Step level = event.getRowValue();
        final Number obj = event.getNewValue();
        final double value = (obj != null) ? obj.doubleValue() : Double.NaN;
        level.value.set(value);
        level.visible.set(true);
        /*
         * Search for index where to move the row in order to keep ascending value order.
         * The algorithm below is okay if the new position is close to current position.
         * A binary search would be more efficient in the general case, but it may not be
         * worth the additional complexity. We do not use `items.sort(…)` because we want
         * to move only one row and its new position will determine the default color.
         */
        final TableView<Step> table = event.getTableView();
        final ObservableList<Step> items = table.getItems();
        final int row = event.getTablePosition().getRow();
        int dst = row;
        while (--dst >= 0) {
            // Use `!` for stopping if `value` is NaN.
            if (!(items.get(dst).value.get() >= value)) break;
        }
        final int size = items.size() - 1;                  // Excluding insertion row.
        while (++dst < size) {
            // No `!` for continuing until the end if `value` is NaN.
            if (dst != row && items.get(dst).value.get() >= value) break;
        }
        if (dst != row) {
            if (dst >= row) dst--;
            items.add(dst, items.remove(row));
            table.getSelectionModel().select(dst);
        }
        if (row >= size) {
            // If the edited line was the insertion row, add a new insertion row.
            items.add(new Step());
        }
        /*
         * If the row has no color (should be the case only for insertion row), interpolate a default color
         * using the steps before and after the new row. If we are at the beginning or end of the list,
         * then interpolation will actually be an extrapolation.
         */
        if (level.color.get() == null) {
            Color color = Color.BLACK;
            final int last = items.size() - 2;      // -1 for excluding insertion row, -1 again for last item.
            if (last >= 2) {                        // Need 3 items: the new row + 2 items for interpolation.
                int ilo = dst - 1;
                int iup = dst + 1;
                if (ilo < 0) {                       // (row index) == 0
                    ilo = 1;
                    iup = 2;
                } else if (iup > last) {             // (row index) == last
                    iup = last - 1;
                    ilo = last - 2;
                }
                final Step lo = items.get(ilo);
                final Step up = items.get(iup);
                final double base = lo.value.get();
                final double f = (value - base) / (up.value.get() - base);
                final Color clo = lo.color.get().color();
                final Color cup = up.color.get().color();
                color = new Color(interpolate(f, clo.getRed(),     cup.getRed()),
                                  interpolate(f, clo.getGreen(),   cup.getGreen()),
                                  interpolate(f, clo.getBlue(),    cup.getBlue()),
                                  interpolate(f, clo.getOpacity(), cup.getOpacity()));
            }
            level.color.set(new ColorRamp(color));
        }
    }

    /**
     * Interpolates or extrapolates a color component. Note: JavaFX provides a
     * {@code Color.interpolate(…)} method, but it does not perform extrapolations.
     *
     * @param  f   factor between 0 and 1 for interpolations, outside that range for extrapolations.
     * @param  lo  color component at f = 0.
     * @param  up  color component at f = 1.
     * @return interpolated or extrapolated color component.
     *
     * @see Color#interpolate(Color, double)
     */
    private static double interpolate(final double f, final double lo, final double up) {
        return Math.max(0, Math.min(1, (up - lo) * f + lo));
    }

    /**
     * Creates a table showing the color of isoline levels.
     * The color can be modified by selecting the table row, then clicking on the color.
     *
     * @param  vocabulary  localized resources, given because already known by the caller
     *                     (this argument would be removed if this method was public API).
     * @return table of isolines.
     */
    public final TableView<Step> createIsolineTable(final Vocabulary vocabulary) {
        /*
         * First column containing a checkbox for choosing whether the isoline should be drawn or not.
         * Header text is 🖉 (lower left pencil).
         */
        final TableColumn<Step,Boolean> visible = new TableColumn<>("\uD83D\uDD89");
        visible.setCellFactory(CheckBoxTableCell.forTableColumn(visible));
        visible.setCellValueFactory((cell) -> cell.getValue().visible);
        visible.setSortable(false);
        visible.setResizable(false);
        visible.setMinWidth(Styles.CHECKBOX_WIDTH);
        visible.setMaxWidth(Styles.CHECKBOX_WIDTH);
        /*
         * Second column containing the level value.
         * The number can be edited using a `NumberFormat` in current locale.
         */
        final TableColumn<Step,Number> level = new TableColumn<>(vocabulary.getString(Vocabulary.Keys.Level));
        final FormatTableCell.Trigger<Step> trigger = new FormatTableCell.Trigger<>(level, format);
        level.setCellFactory((column) -> new FormatTableCell<>(Number.class, format, trigger));
        level.setCellValueFactory((cell) -> cell.getValue().value);
        level.setOnEditCommit(ValueColorMapper::commitEdit);
        level.setSortable(false);                           // We will do our own sorting.
        level.setId("level");
        /*
         * Create the table with above "category name" column (read-only),
         * and add an editable column for color(s).
         */
        final TableView<Step> table = new TableView<>();
        table.getColumns().setAll(visible, level);
        addColumnTo(table, vocabulary.getString(Vocabulary.Keys.Color));
        /*
         * Add an empty row that user can edit for adding new data. This row will automatically enter in edition state
         * when a digit is typed (this is the purpose of `trigger`). For making easier to edit the cell in current row,
         * a listener on F2 key (same as Excel and OpenOffice) is also registered.
         */
        table.getItems().add(new Step());
        trigger.registerTo(table);
        table.setOnKeyPressed(ValueColorMapper::deleteRow);
        return table;
    }

    /**
     * Invoked when user presses a key. If the key is "delete", then the current row is removed.
     */
    private static void deleteRow(final KeyEvent event) {
        final TableView<?> table = (TableView<?>) event.getSource();
        if (event.getCode() == KeyCode.DELETE) {
            final int row = table.getSelectionModel().getSelectedIndex();
            final ObservableList<?> items = table.getItems();
            if (row >= 0 && row < items.size() - 1) {           // Do not delete last row, which is insertion row.
                items.remove(row);
            }
        }
    }
}
