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
package org.apache.sis.referencing.factory.sql.epsg;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.LineNumberReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.apache.sis.util.Workaround;
import org.apache.sis.util.CharSequences;
import org.apache.sis.util.StringBuilders;
import org.apache.sis.util.privy.URLs;
import org.apache.sis.metadata.sql.privy.ScriptRunner;

// Test dependencies
import org.apache.sis.metadata.sql.TestDatabase;


/**
 * Rewrites the {@code INSERT TO ...} statements in a SQL script in a more compact form.
 * This class is used only for updating the SQL scripts used by Apache SIS for the EPSG
 * dataset when a newer release of the EPSG dataset is available.
 * The steps to follow are documented in the {@code README.md} file.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 */
public final class DataScriptFormatter extends ScriptRunner {
    /**
     * Compacts the {@code Data.sql} file provided by EPSG. This method expects two arguments.
     * The first argument is the file of the SQL script to read, which must exist.
     * The second argument is the file where to write the compacted SQL script,
     * which will be overwritten without warning if it exists.
     * The values of those arguments are typically:
     *
     * <ol>
     *   <li>{@code $EPSG_SCRIPTS/PostgreSQL_Data_Script.sql}</li>
     *   <li>{@code $NON_FREE/EPSG/Data.sql}</li>
     * </ol>
     *
     * @param  arguments  the source files and the destination file.
     * @throws Exception if an error occurred while reading of writing the file.
     */
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            System.err.println("Expected two arguments: source SQL file and target SQL file.");
            return;
        }
        try (TestDatabase db = TestDatabase.create("dummy");
             Connection c = db.source.getConnection())
        {
            final var f = new DataScriptFormatter(c);
            f.run(Path.of(arguments[0]), Path.of(arguments[1]));
        }
    }

    /**
     * The {@value} keywords.
     */
    private static final String INSERT_INTO = "INSERT INTO";

    /**
     * The {@value} keywords.
     */
    private static final String VALUES = "VALUES";

    /**
     * Prefix at the beginning of all table names in the SQL script.
     */
    private static final String TABLE_NAME_PREFIX = "epsg_";

    /**
     * Mapping from the table names in SQL scripts to table names in the MS-Access database.
     * The latter is apparently the support on which the EPSG geodetic dataset has been developed,
     * and uses more readable table names. The {@value #TABLE_NAME_PREFIX} prefix is omitted in all keys.
     *
     * @see #renameTables(StringBuilder)
     */
    private final Map<String,String> toOriginalTableNames;

    /**
     * The columns to search for computing {@link TableValues#booleanColumnIndices}.
     */
    private final Set<String> booleanColumns;

    /**
     * The columns to search for computing {@link TableValues#doubleColumnIndices}.
     * We do not reformat the {@code change_id} columns because they are more like character strings.
     * We do not reformat east/west/north/south bounds or {@code greenwich_longitude} because their
     * values are close to integers, or {@code semi_major_axis}, {@code semi_minor_axis} and
     * {@code inv_flattening} for similar reasons.
     */
    private final Set<String> doubleColumns;

    /**
     * The values for each table. The array length is the maximal number of tables that we expect in an EPSG schema.
     * In {@link ArrayIndexOutOfBoundsException} when reading this array means that the schema has changed.
     */
    private final TableValues[] valuesPerTable;

    /**
     * Number of valid values in {@link #valuesPerTable}.
     */
    private int tableCount;

    /**
     * Statements other than {@code INSERT INTO}.
     */
    private final List<String> otherStatements;

    /**
     * Creates a new instance.
     *
     * @param  c  a dummy connection. Will be used for fetching metadata.
     * @throws SQLException if an error occurred while fetching metadata.
     */
    private DataScriptFormatter(final Connection c) throws SQLException {
        super(c, Integer.MAX_VALUE);
        booleanColumns  = Set.of("deprecated", "show_crs", "show_operation", "reverse_op", "param_sign_reversal", "ellipsoid_shape");
        doubleColumns   = Set.of("parameter_value");
        valuesPerTable  = new TableValues[30];
        otherStatements = new ArrayList<>();
        toOriginalTableNames = Map.ofEntries(
                Map.entry("alias",                      "Alias"),
                Map.entry("change",                     "Change"),
                Map.entry("conventionalrs",             "ConventionalRS"),
                Map.entry("coordinateaxis",             "Coordinate Axis"),
                Map.entry("coordinateaxisname",         "Coordinate Axis Name"),
                Map.entry("coordoperation",             "Coordinate_Operation"),
                Map.entry("coordoperationmethod",       "Coordinate_Operation Method"),
                Map.entry("coordoperationparam",        "Coordinate_Operation Parameter"),
                Map.entry("coordoperationparamusage",   "Coordinate_Operation Parameter Usage"),
                Map.entry("coordoperationparamvalue",   "Coordinate_Operation Parameter Value"),
                Map.entry("coordoperationpath",         "Coordinate_Operation Path"),
                Map.entry("coordinatereferencesystem",  "Coordinate Reference System"),
                Map.entry("coordinatesystem",           "Coordinate System"),
                Map.entry("datum",                      "Datum"),
                Map.entry("datumensemble",              "DatumEnsemble"),
                Map.entry("datumensemblemember",        "DatumEnsembleMember"),
                Map.entry("datumrealizationmethod",     "DatumRealizationMethod"),
                Map.entry("definingoperation",          "DefiningOperation"),
                Map.entry("deprecation",                "Deprecation"),
                Map.entry("ellipsoid",                  "Ellipsoid"),
                Map.entry("extent",                     "Extent"),
                Map.entry("namingsystem",               "Naming System"),
                Map.entry("primemeridian",              "Prime Meridian"),
                Map.entry("scope",                      "Scope"),
                Map.entry("supersession",               "Supersession"),
                Map.entry("unitofmeasure",              "Unit of Measure"),
                Map.entry("usage",                      "Usage"),
                Map.entry("versionhistory",             "Version History"));
    }

    /**
     * Returns {@code true} if the given line should be omitted from the script.
     *
     * @param  line  the line, without trailing {@code ';'}.
     * @return {@code true} if the line should be omitted.
     */
    private static boolean omit(final String line) {
        // We omit the following line because we changed the type from VARCHAR to DATE.
        return line.startsWith("UPDATE epsg_datum SET realization_epoch = replace(realization_epoch, CHR(182), CHR(10))");
    }

    /**
     * Compacts the given file.
     *
     * @param  inputFile    the input file where to read the SQL statements to compact.
     * @param  outputFile   the output file where to write the compacted SQL statements.
     * @throws IOException  if an I/O operation failed.
     * @throws SQLException should never happen.
     */
    private void run(final Path inputFile, final Path outputFile) throws SQLException, IOException {
        if (inputFile.equals(outputFile)) {
            throw new IllegalArgumentException("Input and output files are the same.");
        }
        try (LineNumberReader in = new LineNumberReader(new InputStreamReader(Files.newInputStream(inputFile), StandardCharsets.UTF_8))) {
            run(inputFile.getFileName().toString(), in);
        }
        try (BufferedWriter out = Files.newBufferedWriter(outputFile)) {
            out.write("---\n" +
                      "---    Copyright International Association of Oil and Gas Producers (IOGP)\n" +
                      "---    See  " + URLs.EPSG_LICENSE + "  (a copy is in ./LICENSE.txt).\n" +
                      "---\n" +
                      "---    This file has been reformatted for the needs of Apache SIS project.\n" +
                      "---    See org.apache.sis.referencing.factory.sql.epsg.DataScriptFormatter.\n" +
                      "---\n" +
                      "\n");

            Arrays.sort(valuesPerTable, 0, tableCount);
            for (int i=0; i<tableCount; i++) {
                valuesPerTable[i].write(out);
            }
            for (String other : otherStatements) {
                out.write(other);
                out.write(";\n");       // Really want Unix EOL, not the platform-specific one.
            }
        }
    }

    /**
     * EPSG scripts version 8.9 seems to have 2 errors where the {@code OBJECT_TABLE_NAME} column contains
     * {@code "AxisName"} instead of {@code "Coordinate Axis Name"}. Furthermore, the version number noted
     * in the history table is a copy-and-paste error.
     *
     * @param  sql    the whole SQL statement.
     * @param  lower  index of the opening quote character ({@code '}) of the text in {@code sql}.
     * @param  upper  index after the closing quote character ({@code '}) of the text in {@code sql}.
     */
    @Override
    @Workaround(library="EPSG", version="8.9")
    protected void editText(final StringBuilder sql, int lower, int upper) {
        lower++;
        upper--;                    // Skip the single quotes.
        final String table;         // Name of the table where to replace a value.
        final String before;        // String that must exist before the value to replace, or null if none.
        final String oldValue;      // The old value to replace.
        final String newValue;      // The new value.
        switch (upper - lower) {    // Optimization for reducing the number of comparisons.
            default: return;
            case 8: {
                table    = "epsg_deprecation";
                before   = null;
                oldValue = "AxisName";
                newValue = "Coordinate Axis Name";
                break;
            }
            case 36: {
                table    = "epsg_versionhistory";
                before   = "'8.9'";
                oldValue = "Version 8.8 full release of Dataset.";
                newValue = "Version 8.9 full release of Dataset.";
                break;
            }
        }
        if (CharSequences.regionMatches(sql, lower, oldValue)) {
            final int s = CharSequences.skipLeadingWhitespaces(sql, 0, lower);
            if (CharSequences.regionMatches(sql, s, "INSERT INTO " + table + " VALUES")) {
                if (upper - lower != oldValue.length()) {
                    throw new AssertionError("Unexpected length");
                }
                if (before != null) {
                    final int i = sql.indexOf(before);
                    if (i < 0 || i >= lower) return;
                }
                sql.replace(lower, upper, newValue);
            }
        }
        StringBuilders.trimWhitespaces(sql, lower, upper);
    }

    /**
     * Makes sure that {@link #execute(StringBuilder)} is invoked for every line. Whether the SQL statement
     * is supported or not is irrelevant for this method since we do not know yet what will be the database
     * engine. We just copy the SQL statements in a file without executing them.
     *
     * @return {@code true}.
     */
    @Override
    protected boolean isSupported(final CharSequence sql) {
        return true;
    }

    /**
     * "Executes" the given SQL statement. In the context of this {@code EPSGDataWriter} class,
     * executing a SQL statement means compacting it and writing it to the output file.
     *
     * @param  sql  the SQL statement to compact.
     * @return the number of rows added.
     * @throws IOException if an I/O operation failed.
     * @throws SQLException if a syntax error happens.
     */
    @Override
    protected int execute(final StringBuilder sql) throws IOException, SQLException {
        removeLF(sql);
        renameTables(sql);
        String line = CharSequences.trimWhitespaces(sql).toString();
        if (line.startsWith("UPDATE ")) {
            /*
             * Some EPSG tables have a "table_name" field which will contain the names of other EPSG tables.
             * In the EPSG scripts, the values are initially the table names used in the MS-Access database.
             * Then the MS-Access table names are replaced by statements like below:
             *
             *    UPDATE epsg_alias SET object_table_name = 'epsg_coordinateaxis' WHERE object_table_name = 'Coordinate Axis';
             *    UPDATE epsg_deprecation SET object_table_name = 'epsg_alias' WHERE object_table_name = 'Alias';
             *    etc.
             *
             * For Apache SIS, we keep the original table names as defined in MS-Access database,
             * for consistency with the table names that we actually use in our EPSG schema.
             */
            if (line.contains("object_table_name")) {
                return 0;
            }
            /*
             * Following statements do not make sense anymore on enumerated or boolean values:
             *
             *    UPDATE epsg_coordinatereferencesystem SET coord_ref_sys_kind = replace(coord_ref_sys_kind, CHR(182), CHR(10));
             *    UPDATE epsg_coordinatesystem SET coord_sys_type = replace(coord_sys_type, CHR(182), CHR(10));
             *    UPDATE epsg_datum SET datum_type = replace(datum_type, CHR(182), CHR(10));
             *    UPDATE epsg_coordoperationparamusage SET param_sign_reversal = replace(param_sign_reversal, CHR(182), CHR(10))
             */
            if (line.contains("replace")) {
                if (line.contains("param_sign_reversal") || line.contains("coord_ref_sys_kind")
                        || line.contains("coord_sys_type") || line.contains("datum_type"))
                {
                    return 0;
                }
            }
        }
        /*
         * Search for a table in reverse order on the assumption that the most recently added tables
         * are the most likely to be reused in the next statement.
         */
        for (int i = tableCount; --i >= 0;) {
            final TableValues values = valuesPerTable[i];
            if (line.startsWith(values.insertStatement)) {
                // The previous instruction was already an INSERT INTO the same table.
                values.add(line);
                return 1;
            }
        }
        if (line.startsWith(INSERT_INTO)) {
            /*
             * We are beginning insertions in a new table.
             */
            int valuesStart = line.indexOf(VALUES, INSERT_INTO.length());
            if (valuesStart < 0) {
                throw new SQLException("This simple program wants VALUES on the same line as INSERT INTO.");
            }
            valuesStart += VALUES.length();     // Move to the end of "VALUES".
            final CharSequence insertStatement = CharSequences.trimWhitespaces(line, 0, valuesStart);
            final var values = new TableValues(insertStatement.toString(), booleanColumns, doubleColumns);
            valuesPerTable[tableCount++] = values;
            values.add(line);
            return 1;
        }
        if (!omit(line)) {
            otherStatements.add(line);
        }
        return 0;
    }

    /**
     * Reformats a multi-line text as a single line text. For each occurrence of line feed
     * (the {@code '\n'} character) found in the given buffer, this method performs the following steps:
     *
     * <ol>
     *   <li>Remove the line feed character and the {@linkplain Character#isWhitespace(char) white spaces} around them.</li>
     *   <li>If the last character before the line feed and the first character after the line feed are both
     *       {@linkplain Character#isLetterOrDigit(char) letter or digit}, then a space is inserted between them.
     *       Otherwise they will be no space.</li>
     * </ol>
     *
     * This method is provided for {@link #execute(StringBuilder)} implementations, in order to "compress"
     * a multi-lines SQL statement on a single line before further processing by the caller.
     *
     * <p><b>Note:</b> current version does not use codepoint API
     * on the assumption that it is not needed for EPSG's SQL files.</p>
     *
     * @param  sql  the string in which to perform the removal.
     */
    static void removeLF(final StringBuilder sql) {
        int i = sql.length();
        while ((i = sql.lastIndexOf("\n", i)) >= 0) {
            final int length = sql.length();
            int nld = 0;
            int upper = i;
            while (++upper < length) {
                final char c = sql.charAt(upper);
                if (!Character.isWhitespace(c)) {
                    if (Character.isLetterOrDigit(c)) {
                        nld++;
                    }
                    break;
                }
            }
            while (i != 0) {
                final char c = sql.charAt(--i);
                if (!Character.isWhitespace(c)) {
                    if (Character.isLetterOrDigit(c)) {
                        nld++;
                    }
                    i++;
                    break;
                }
            }
            if (nld == 2) {
                upper--;
            }
            sql.delete(i, upper);
            if (nld == 2) {
                sql.setCharAt(i, ' ');
            }
        }
    }

    /**
     * Renames the tables from the {@code "epsg_foo"} pattern to the more readable names used in MS-Access database.
     *
     * @param  sql  the SQL statement to edit.
     */
    private void renameTables(final StringBuilder sql) {
        int start, limit = sql.length();
        while ((start = sql.lastIndexOf(TABLE_NAME_PREFIX, limit)) >= 0) {
            final int afterPrefix = start + TABLE_NAME_PREFIX.length();
            int end = afterPrefix;
            char c = 0;
            while (end < limit && Character.isLetter(c = sql.charAt(end))) end++;
            /*
             * The table name can appear either as a value of type VARCHAR, or an identifier in an INSERT statement.
             * We replace the table name only in the former case for consistency with `epsg_table_name` enumeration.
             * We keep the "epsg_foo" table names because they are easier to filter.
             */
            if (c == '\'') {
                String table = toOriginalTableNames.get(sql.substring(afterPrefix, end));
                if (table != null) {
                    sql.replace(start, end, table);
                }
            }
            limit = start - 1;
        }
    }
}
