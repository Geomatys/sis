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
package org.apache.sis.gui.dataset;

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import javafx.geometry.Pos;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableView;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TitledPane;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.util.StringConverter;
import org.apache.sis.gui.Widget;
import org.apache.sis.storage.Resource;
import org.apache.sis.util.resources.Vocabulary;
import org.apache.sis.internal.gui.Styles;
import org.apache.sis.internal.gui.LogHandler;
import org.apache.sis.internal.gui.ExceptionReporter;
import org.apache.sis.internal.gui.ImmutableObjectProperty;
import org.apache.sis.util.logging.PerformanceLevel;
import org.apache.sis.util.CharSequences;


/**
 * Shows a table of recent log records, optionally filtered to logs related to a specific resource.
 * By default, {@code LogViewer} does not show any log entry.
 * For viewing logs, one of the two following actions must be done:
 *
 * <ul>
 *   <li>Set {@link #source} to a non-null {@link Resource} value.</li>
 *   <li>Set {@link #systemLogs} to {@code true}.</li>
 * </ul>
 *
 * When a {@link Resource} value is specified, {@code LogViewer} shows only some logs that occurred while
 * using that resource, in particular {@linkplain Resource#addListener warnings emitted by the resource}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public class LogViewer extends Widget {
    /**
     * Spaces between labels and controls, in pixels.
     */
    private static final int SPACE = 6;

    /**
     * Space between {@link #message} and the log record identification
     * (the lines ending with {@link #method}).
     */
    private static final Insets MARGIN = new Insets(SPACE, 0, 0, 0);

    /**
     * Space around the button bar.
     */
    private static final Insets BAR_INSETS = new Insets(SPACE);

    /**
     * Localized string representations of {@link Level}.
     * This map shall be read and written from JavaFX thread only.
     *
     * @see #toString(Level)
     */
    private static final Map<Level,String> LEVEL_NAMES = new HashMap<>(12);

    /**
     * The table of log records.
     */
    private final TableView<LogRecord> table;

    /**
     * The view combining the table with details about the selected record.
     *
     * @see #getView()
     */
    private final SplitPane view;

    /**
     * Details about selected record.
     */
    private final Label level, time, logger, classe, method;

    /**
     * Area where to show the log message.
     */
    private final TextArea message;

    /**
     * The data store or resource for which to show log records.
     * If this property value is {@code null}, then the system logs will be shown
     * if {@link #systemLogs} is {@code true}, or no logs will be shown otherwise.
     *
     * @see Resource#addListener(Class, StoreListener)
     */
    public final ObjectProperty<Resource> source;

    /**
     * Whether to show system logs instead then the logs related to a specific resource.
     * If this property is set to {@code true}, then {@link #source} is automatically set to {@code null}.
     * Conversely if {@link #source} is set to a non-null value, then this property is set to {@code false}.
     */
    public final BooleanProperty systemLogs;

    /**
     * Whether this viewer has no log record to show.
     *
     * @see #isEmptyProperty()
     */
    private final IsEmpty isEmpty;

    /**
     * Whether {@link #source} is modified in reaction to a {@link #systemLogs} change, or conversely.
     */
    private boolean isAdjusting;

    /**
     * The formatter for logging messages.
     */
    private final SimpleFormatter formatter;

    /**
     * Format for dates and times using a short or long representation. The short representation is for
     * a column in the table, and the long representation is for the details panel below the table.
     */
    private final DateFormat shortDates, longDates;

    /**
     * The button for showing the main message or the stack trace.
     */
    private final ToggleButton messageButton, traceButton;

    /**
     * Filters log record according current settings in the button bar.
     * May be {@code null} if there is no filter.
     *
     * @see FilteredList#predicateProperty()
     */
    private Predicate<LogRecord> filter;

    /**
     * Creates an initially empty viewer of log records. For viewing logs, {@link #source}
     * must be set to a non-null value or {@link #systemLogs} must be set to {@code true}.
     */
    public LogViewer() {
        this(Vocabulary.getResources((Locale) null));
    }

    /**
     * Creates a new view of log records.
     */
    LogViewer(final Vocabulary vocabulary) {
        source     = new SimpleObjectProperty<>(this, "source");
        systemLogs = new SimpleBooleanProperty (this, "systemLogs");
        isEmpty    = new IsEmpty(this);
        formatter  = new SimpleFormatter();
        shortDates = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, vocabulary.getLocale());
        longDates  = DateFormat.getDateTimeInstance(DateFormat.LONG,  DateFormat.LONG,  vocabulary.getLocale());
        table      = new TableView<>(FXCollections.emptyObservableList());
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        table.setTableMenuButtonVisible(true);
        table.getColumns().setAll(column(vocabulary, Vocabulary.Keys.Level),
                                  column(vocabulary, Vocabulary.Keys.DateAndTime),
                                  column(vocabulary, Vocabulary.Keys.Logger),
                                  column(vocabulary, Vocabulary.Keys.Class),
                                  column(vocabulary, Vocabulary.Keys.Method),
                                  column(vocabulary, Vocabulary.Keys.Message));
        /*
         * Details pane to be shown below the table. Provides details about the selected record.
         */
        final GridPane    details;
        final ToggleGroup buttonGroup;
        {
            final Font font = Font.font(null, FontWeight.SEMI_BOLD, -1);
            details = Styles.createControlGrid(0,
                    label(font, vocabulary, Vocabulary.Keys.Level,       level  = new Label()),
                    label(font, vocabulary, Vocabulary.Keys.DateAndTime, time   = new Label()),
                    label(font, vocabulary, Vocabulary.Keys.Logger,      logger = new Label()),
                    label(font, vocabulary, Vocabulary.Keys.Class,       classe = new Label()),
                    label(font, vocabulary, Vocabulary.Keys.Method,      method = new Label()));

            messageButton = new ToggleButton(vocabulary.getString(Vocabulary.Keys.Message));
            traceButton   = new ToggleButton(vocabulary.getString(Vocabulary.Keys.Trace));
            messageButton.setSelected(true);
            messageButton.setMaxWidth(Double.MAX_VALUE);
            traceButton  .setMaxWidth(Double.MAX_VALUE);
            buttonGroup = new ToggleGroup();
            buttonGroup.getToggles().setAll(messageButton, traceButton);
            final VBox textSelector = new VBox(SPACE, messageButton, traceButton);

            message = new TextArea();
            message.setEditable(false);
            GridPane.setConstraints(textSelector, 0, 5);
            GridPane.setConstraints(message, 1, 5);
            GridPane.setMargin(textSelector, MARGIN);
            GridPane.setMargin(message, MARGIN);
            details.getChildren().addAll(textSelector, message);
            details.setVgap(0);
        }
        /*
         * Buttons bar on top of the table. Provides filtering options.
         */
        final VBox tableAndBar;
        {
            final Label label = new Label(vocabulary.getLabel(Vocabulary.Keys.Level));
            final ChoiceBox<Level> levels = new ChoiceBox<>();
            label.setLabelFor(levels);
            final HBox bar = new HBox(SPACE, label, levels);
            bar.setAlignment(Pos.CENTER_LEFT);
            bar.setPadding(BAR_INSETS);
            tableAndBar = new VBox(bar, table);
            VBox.setVgrow(table, Priority.ALWAYS);

            levels.getItems().setAll(Level.SEVERE, Level.WARNING, Level.INFO, Level.CONFIG,
                                     PerformanceLevel.SLOW, Level.FINE, Level.FINER, Level.ALL);
            levels.setConverter(Converter.INSTANCE);
            levels.getSelectionModel().select(Level.ALL);
            levels.getSelectionModel().selectedItemProperty().addListener((p,o,n) -> setFilter(n));
        }
        /*
         * Put all view components together.
         */
        view = new SplitPane(tableAndBar, new TitledPane(vocabulary.getString(Vocabulary.Keys.Details), details));
        view.setOrientation(Orientation.VERTICAL);
        SplitPane.setResizableWithParent(details, false);
        /*
         * Register all remaining listeners.
         */
        source.addListener((p,o,n) -> {
            if (!isAdjusting) try {
                isAdjusting = true;
                systemLogs.set(false);
                setItems(LogHandler.getRecords(n));
            } finally {
                isAdjusting = false;
            }
        });
        systemLogs.addListener((p,o,n) -> {
            if (!isAdjusting) try {
                isAdjusting = true;
                source.set(null);
                setItems(n ? LogHandler.getSystemRecords() : null);
            } finally {
                isAdjusting = false;
            }
        });
        final ReadOnlyObjectProperty<LogRecord> selected = table.getSelectionModel().selectedItemProperty();
        buttonGroup.selectedToggleProperty().addListener((p,o,n) -> setMessageOrTrace(selected.get()));
        selected.addListener((p,o,n) -> selected(n));
    }

    /**
     * Creates a column and register its cell factory.
     * This is a helper method for the constructor.
     */
    private TableColumn<LogRecord, String> column(final Vocabulary vocabulary, final short key) {
        final TableColumn<LogRecord, String> column = new TableColumn<>(vocabulary.getString(key));
        column.setCellValueFactory((cell) -> toString(cell, key));
        column.setVisible(key == Vocabulary.Keys.Message);
        return column;
    }

    /**
     * Creates a label of the "details" pane.
     * This is a helper method for the constructor.
     */
    private static Label label(final Font font, final Vocabulary vocabulary, final short key, final Label content) {
        final Label label = new Label(vocabulary.getLabel(key));
        label.setLabelFor(content);
        label.setFont(font);
        return label;
    }

    /**
     * Sets a new list of log records.
     *
     * @param  records  the new list of records, or {@code null} if none.
     */
    private void setItems(final ObservableList<LogRecord> records) {
        if (records == null) {
            table.setItems(FXCollections.emptyObservableList());
        } else {
            final boolean e = records.isEmpty();
            table.setItems(new FilteredList<>(records, filter));
            isEmpty.set(e);
            if (e) {
                records.addListener(isEmpty);
            }
        }
    }

    /**
     * Implementation of {@link LogViewer#isEmpty} property.
     * Also a listener for being notified when the property value needs to be changed.
     */
    private static final class IsEmpty extends ReadOnlyBooleanWrapper implements ListChangeListener<LogRecord> {
        /**
         * Creates the {@link LogViewer#isEmpty} property.
         */
        IsEmpty(final LogViewer owner) {
            super(owner, "isEmpty", true);
        }

        /**
         * Invoked when the list of records changed.
         */
        @Override public void onChanged(final Change<? extends LogRecord> change) {
            final ObservableList<? extends LogRecord> list = change.getList();
            if (!list.isEmpty()) {
                list.removeListener(this);
            }
            set(false);
        }
    }

    /**
     * Whether this viewer has no log record to show.
     * This property is useful for disabling or enabling a tab.
     *
     * @return the property telling whether this viewer no log record to show.
     */
    public final ReadOnlyBooleanProperty isEmptyProperty() {
        return isEmpty.getReadOnlyProperty();
    }

    /**
     * Converter from {@link Level} to localized string representation.
     */
    private static final class Converter extends StringConverter<Level> {
        /** The unique instance. */
        static final Converter INSTANCE = new Converter();

        /** Constructs the unique instance. */
        private Converter() {}

        /** Returns the string representation of given level. */
        @Override public String toString(Level level) {return LogViewer.toString(level);}

        /** Converse of {@link #toString(Level)}. */
        @Override public Level fromString(final String text) {
            for (final Map.Entry<Level,String> entry : LEVEL_NAMES.entrySet()) {
                if (entry.getValue().equals(text)) {
                    return entry.getKey();
                }
            }
            return null;
        }
    }

    /**
     * Returns the localized string representations of given {@link Level}.
     */
    private static String toString(final Level level) {
        if (level == null) {
            return null;
        }
        return LEVEL_NAMES.computeIfAbsent(level, (v) -> {
            final short key;
            if (Level.INFO.equals(v)) {
                key = Vocabulary.Keys.Information;
            } else if (Level.CONFIG.equals(v)) {
                key = Vocabulary.Keys.Configuration;
            } else {
                return CharSequences.upperCaseToSentence(v.getLocalizedName()).toString();
            }
            return Vocabulary.format(key);
        });
    }

    /**
     * Returns the string representation of a logger property for the given cell.
     */
    private ObservableValue<String> toString(final CellDataFeatures<LogRecord,String> cell, final short type) {
        if (cell != null) {
            final LogRecord log = cell.getValue();
            if (log != null) {
                String text;
                switch (type) {
                    case Vocabulary.Keys.Level: {
                        text = log.getLevel().getLocalizedName();
                        break;
                    }
                    case Vocabulary.Keys.DateAndTime: {
                        text = shortDates.format(new Date(log.getMillis()));
                        break;
                    }
                    case Vocabulary.Keys.Logger: {
                        text = log.getLoggerName();
                        break;
                    }
                    case Vocabulary.Keys.Class: {
                        text = log.getSourceClassName();
                        if (text != null) {
                            text = text.substring(text.lastIndexOf('.') + 1);
                        }
                        break;
                    }
                    case Vocabulary.Keys.Method: {
                        text = log.getSourceMethodName();
                        break;
                    }
                    case Vocabulary.Keys.Message: {
                        text = formatter.formatMessage(log);
                        break;
                    }
                    default: throw new AssertionError(type);
                }
                if (text != null) {
                    return new ImmutableObjectProperty<>(text);
                }
            }
        }
        return null;
    }

    /**
     * Invoked when a log record is selected.
     */
    private void selected(final LogRecord log) {
        String level = null, time = null, logger = null, classe = null, method = null;
        if (log != null) {
            level   = toString(log.getLevel());
            time    = longDates.format(new Date(log.getMillis()));
            logger  = log.getLoggerName();
            classe  = log.getSourceClassName();
            method  = log.getSourceMethodName();
            final boolean td = (log.getThrown() == null);
            traceButton.setDisable(td);
            if (td) {
                messageButton.setSelected(true);
            }
        }
        this.level  .setText(level);
        this.time   .setText(time);
        this.logger .setText(logger);
        this.classe .setText(classe);
        this.method .setText(method);
        setMessageOrTrace(log);
    }

    /**
     * Sets the text or the exception stack trace, depending which button is selected.
     */
    private void setMessageOrTrace(final LogRecord log) {
        String text = null;
        if (log != null) {
            if (messageButton.isSelected()) {
                message.setWrapText(true);
                text = formatter.formatMessage(log);
            } else if (traceButton.isSelected()) {
                message.setWrapText(false);
                final Throwable exception = log.getThrown();
                if (exception != null) {
                    text = ExceptionReporter.getStackTrace(exception);
                }
            }
        }
        message.setText(text);
    }

    /**
     * Sets the filter to the given setting. Currently sets only the logging level,
     * but more configuration may be added in the future.
     *
     * @param  level  the new level, or {@code null} if unchanged/
     */
    private void setFilter(final Level level) {
        if (level != null) {
            if (Level.ALL.equals(level)) {
                filter = null;
            } else {
                filter = (log) -> log != null && log.getLevel().intValue() >= level.intValue();
            }
            final ObservableList<LogRecord> items = table.getItems();
            if (items instanceof FilteredList<?>) {
                ((FilteredList<LogRecord>) items).setPredicate(filter);
            }
        }
    }

    /**
     * Returns the control to show in the scene graph.
     * The implementation class may change in any future version.
     */
    @Override
    public Region getView() {
        return view;
    }
}
