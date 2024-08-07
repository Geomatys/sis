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
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.sis.util.CharSequences;
import org.apache.sis.util.StringBuilders;


/**
 * The values associated to an {@code INSERT INTO "Table"} statement.
 *
 * @author  Martin Desruisseaux (IRD, Geomatys)
 */
final class TableValues implements Comparable<TableValues>, Comparator<String> {
    /**
     * The {@code INSERT INTO "Table"} statement currently executing.
     * Used in order to detect when the script start inserting values in a different table.
     */
    final String insertStatement;

    /**
     * Indices (in reversal order) of columns to change from type SMALLINT to type BOOLEAN.
     * Index 0 is the last column, index 1 is the column before the last, <i>etc</i>.
     * We use the reverse order because most boolean columns in the EPSG dataset are last.
     */
    private final int[] booleanColumnIndices;

    /**
     * Indices (in reversal order) of columns to reformat as a double value.
     * The EPSG database contains values like {@code -0.000000000000000000000003689471323},
     * which are either not parsed by Derby because too long (Derby 10.14 documents a limit
     * of 30 characters for floating-point constants) or parsed as 0. To workaround such
     * limitation, we reformat above value as {@code -3.689471323E-24}.
     */
    private final int[] doubleColumnIndices;

    /**
     * The values.
     */
    private final List<String> data;

    /**
     * Creates an initially empty set of values.
     */
    TableValues(final String insertStatement, final Set<String> booleanColumns, final Set<String> doubleColumns) {
        this.insertStatement = insertStatement;
        final CharSequence[] columns = CharSequences.split(
                insertStatement.substring(
                        insertStatement.indexOf('(') + 1,
                        insertStatement.lastIndexOf(')')), ',');

        booleanColumnIndices = indices(columns, booleanColumns);
        doubleColumnIndices  = indices(columns, doubleColumns);
        data = new ArrayList<>(1000);
    }

    /**
     * Returns the indices where the given columns are found, in reverse order.
     * Indices are returned in reverse order (0 is the last column) for historical reasons,
     * because a previous version of {@link DataScriptFormatter} was using hard-coded indices.
     */
    private static int[] indices(final CharSequence[] columns, final Set<String> search) {
        final int[] indices = new int[columns.length];
        int n = 0;
        for (int i = columns.length; --i >= 0;) {
            if (search.contains(columns[i])) {
                indices[n++] = (columns.length - 1) - i;
            }
        }
        return Arrays.copyOfRange(indices, 0, n);
    }

    /**
     * Used for sorting tables in alphabetical order before to write them.
     */
    @Override
    public int compareTo(final TableValues other) {
        return insertStatement.compareTo(other.insertStatement);
    }

    /**
     * Used for sorting rows in increasing order of identifiers.
     * The identifiers are usually integers, but sometime use a {@code majpor.minor} pattern.
     */
    @Override
    public int compare(final String line1, final String line2) {
        int s1 = stop(line1, 0);
        int s2 = stop(line2, 0);
        if (s1 == 0) return (s2 == 0) ? line1.compareTo(line2) : +1;
        if (s2 == 0) return -1;  // Sort lines without identifier last.
        int c = Integer.parseInt(line1, 0, s1, 10)
              - Integer.parseInt(line2, 0, s2, 10);
        if (c == 0) {
            final boolean p1 = line1.charAt(s1) == '.';
            final boolean p2 = line2.charAt(s2) == '.';
            if (p1 & p2) {
                // We do not bother to check if a number exists. It should always be the case.
                c = Integer.parseInt(line1, ++s1, stop(line1, s1), 10)
                  - Integer.parseInt(line2, ++s2, stop(line2, s2), 10);
            } else {
                if (p1) return +1;
                if (p2) return -1;
            }
        }
        return c;
    }

    /**
     * Returns the index after the last digit, or {@code offset} if none.
     *
     * @param  line    the line to scan.
     * @param  offset  first index to scan.
     * @return index after the last digit.
     */
    private static int stop(final String line, int offset) {
        final int length = line.length();
        while (offset < length) {
            final char c = line.charAt(offset);
            if (c < '0' || c > '9') break;
            offset++;
        }
        return offset;
    }

    /**
     * Writes the {@code INSERT INTO} statement followed by all values.
     * This is invoked for writing the final result after all lines have been added.
     */
    final void write(final BufferedWriter out) throws IOException {
        data.sort(this);
        String separator = insertStatement + '\n';
        for (String value : data) {
            out.write(separator);
            out.write('(');
            out.write(value);
            out.write(')');
            separator = ",\n";      // Really want Unix EOL, not the platform-specific one.
        }
        out.write(";\n");
    }

    /**
     * Modifies the given {@code INSERT INTO table VALUE (…)} line before to append it.
     * The given line is only the {@code VALUE (…)} part for a single entry.
     * The modifications applied on entry values can be a change of integer types to
     * Boolean types, and rewrite of some floating point values.
     */
    final void add(String line) throws SQLException {
        line = removeUselessExponents(line);

        int start, end;
        end   = line.length();
        start = CharSequences.skipLeadingWhitespaces(line, insertStatement.length(), end);
        end   = CharSequences.skipTrailingWhitespaces(line, start, end);
        if (line.charAt(start++) != '(' || line.charAt(--end) != ')') {
            throw new SQLException("Illegal `VALUES` statement:" + System.lineSeparator() + line);
        }
        start = CharSequences.skipLeadingWhitespaces(line, start, end);
        end   = CharSequences.skipTrailingWhitespaces(line, start, end);
        final var buffer = new StringBuilder(end - start).append(line, start, end);

        end = buffer.length();
        int booleanIndex = 0;
        int doubleIndex  = 0;
        for (int columnIndex=0; end > 0; columnIndex++) {
            /*
             * Find the indices where each column starts and ends. If the value is a character
             * sequence, the range includes the ' quotes. Escaped quotes ('') are supported.
             */
            start = end;
            for (int c; start > 0 && (c = buffer.codePointBefore(start)) != ',';) {
                start -= Character.charCount(c);
                if (c == '\'') {
                    for (;;) {
                        c = buffer.codePointBefore(start);
                        start -= Character.charCount(c);
                        if (c == '\'') {
                            if (buffer.codePointBefore(start) != '\'') {
                                break;
                            }
                            start--;
                        }
                    }
                }
            }
            /*
             * Eventually reformat the column value. We replace empty strings by Null because
             * we replaced some VARCHAR by DATE, and an empty string is an invalid date.
             */
            String value = CharSequences.trimWhitespaces(buffer, start, end).toString();
            if (value.equals("''")) {
                value = "Null";
            } else if (booleanIndex < booleanColumnIndices.length && columnIndex == booleanColumnIndices[booleanIndex]) {
                value = replaceIntegerByBoolean(value);
                booleanIndex++;
            } else if (doubleIndex < doubleColumnIndices.length && columnIndex == doubleColumnIndices[doubleIndex]) {
                value = reformatFloatingPoints(value);
                doubleIndex++;
            }
            // Do the replacement even if the value didn't changed because it may remove spaces.
            buffer.replace(start, end, value);
            end = CharSequences.skipTrailingWhitespaces(buffer, 0, start - 1);
        }
        StringBuilders.trimWhitespaces(buffer, 0, buffer.length());
        data.add(buffer.toString());
    }

    /**
     * Replaces the last {@code SMALLINT} types by {@code BOOLEAN}.
     * This is for consistency with the table type documented in the class javadoc.
     */
    private static String replaceIntegerByBoolean(String value) throws SQLException {
        if (value.equals("0") || value.equalsIgnoreCase("'No'")) {
            return "false";
        } else if (value.equals("1") || value.equalsIgnoreCase("'Yes'")) {
            return "true";
        } else if (value.equalsIgnoreCase("Null") || value.equals("''")) {
            return "Null";
        } else {
            throw new SQLException("Unexpected Boolean value \"" + value + "\".");
        }
    }

    /**
     * Reformats the given floating point number. This is used for replacing for example
     * {@code -0.000000000000000000000003689471323} by {@code -3.689471323E-24}.
     */
    private static String reformatFloatingPoints(String value) {
        if (!value.equalsIgnoreCase("Null")) {
            value = Double.toString(Double.parseDouble(value));
            value = CharSequences.trimFractionalPart(value).toString();
        }
        return value;
    }

    /**
     * For private usage by the following method only.
     */
    private static final Pattern uselessExponentPattern =
            Pattern.compile("([\\(\\,]\\-?\\d+\\.\\d+)E[\\+\\-]?0+([\\,\\)])");

    /**
     * Removes the useless "E0" exponents after floating point numbers.
     */
    private static String removeUselessExponents(String line) {
        StringBuilder cleaned = null;
        final Matcher matcher = uselessExponentPattern.matcher(line);
        while (true) {
            int lastIndex = 0;
            while (matcher.find()) {
                // Make sure this is not a quoted text.
                boolean quoted = false;
                for (int i=matcher.start(); (i=line.lastIndexOf('\'', i-1)) >= 0;) {
                    if (i == 0 || line.charAt(i-1) != '\\') {
                        quoted = !quoted;
                    }
                }
                if (!quoted) {
                    // Found a number outside quotes. Replace.
                    if (cleaned == null) {
                        cleaned = new StringBuilder();
                    }
                    cleaned.append(line, lastIndex, matcher.end(1));
                    lastIndex = matcher.end();
                    cleaned.append(line, matcher.start(2), lastIndex);
                }
            }
            if (lastIndex == 0) {
                return line;
            }
            cleaned.append(line, lastIndex, line.length());
            line = cleaned.toString();
            matcher.reset(line);
            cleaned.setLength(0);
        }
    }
}
