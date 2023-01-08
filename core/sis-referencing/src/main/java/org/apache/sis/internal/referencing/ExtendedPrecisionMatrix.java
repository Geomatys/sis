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
package org.apache.sis.internal.referencing;

import org.apache.sis.util.resources.Errors;
import org.opengis.referencing.operation.Matrix;


/**
 * A matrix capable to store elements with extended precision.
 * Apache SIS uses double-double arithmetic for extended precision,
 * but we want to hide that implementation details from public API.
 * The double-double arithmetic is implemented by a specialized {@link Number} subclass.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.4
 * @since   0.5
 */
public interface ExtendedPrecisionMatrix extends Matrix {
    /**
     * A sentinel value for {@link org.apache.sis.referencing.operation.matrix.Matrices#create(int, int, Number[])}
     * meaning that we request an extended precision matrix with all elements initialized to zero.
     * This is a non-public feature because we try to hide our extended-precision mechanism from the users.
     */
    Number[] CREATE_ZERO = new Number[0];

    /**
     * A sentinel value for {@link org.apache.sis.referencing.operation.matrix.Matrices#create(int, int, Number[])}
     * meaning that we request an extended precision matrix initialized to the identity (or diagonal) matrix.
     * This is a non-public feature because we try to hide our extended-precision mechanism from the users.
     */
    Number[] CREATE_IDENTITY = new Number[0];

    /**
     * Returns the given matrix as an extended precision matrix if possible, or {@code other} otherwise.
     *
     * @param  source  the matrix to cast.
     * @param  other   the value to return if the source is not an instance of {@code ExtendedPrecisionMatrix}.
     * @return the given matrix or {@code other}.
     */
    static ExtendedPrecisionMatrix castOrElse(final Matrix source, final ExtendedPrecisionMatrix other) {
        return (source instanceof ExtendedPrecisionMatrix) ? (ExtendedPrecisionMatrix) source : other;
    }

    /**
     * Returns {@code true} if the given number is zero.
     * This is the criterion used for identifying which {@link Number} elements shall be {@code null}.
     *
     * @param  element  the value to test (may be {@code null}).
     * @return whether the given value is zero or null.
     */
    static boolean isZero(final Number element) {
        return (element == null) || element.doubleValue() == 0;
    }

    /**
     * Returns all matrix elements in a flat, row-major (column indices vary fastest) array.
     * The array length is <code>{@linkplain #getNumRow()} * {@linkplain #getNumCol()}</code>.
     * Zero values <em>shall</em> be null.
     *
     * <p>The {@code writable} argument shall be {@code true} if the caller may write in the returned array.
     * If {@code false}, then the returned array shall be considered read-only because it may be a reference
     * to the array stored internally by this matrix.</p>
     *
     * @param  writable  whether the caller may write in the returned array.
     * @return a copy of all current matrix elements in a row-major array.
     */
    default Number[] getElementAsNumbers(final boolean writable) {
        final int numCol = getNumCol();
        final Number[] elements = new Number[getNumRow() * numCol];
        for (int i=0; i<elements.length; i++) {
            elements[i] = getElementOrNull(i / numCol, i % numCol);
        }
        return elements;
    }

    /**
     * Retrieves the value at the specified row and column if different than zero.
     * If the value is zero, then this method <em>shall</em> return {@code null}.
     * The use of {@code null} for zero is a way to identify zero easily no matter
     * the value type.
     *
     * @param  row     the row index, from 0 inclusive to {@link #getNumRow()} exclusive.
     * @param  column  the column index, from 0 inclusive to {@link #getNumCol()} exclusive.
     * @return the current value at the given row and column, or {@code null} if the value is zero.
     * @throws IndexOutOfBoundsException if the specified row or column is out of bounds.
     */
    Number getElementOrNull(int row, int column);

    /**
     * Retrieves the value at the specified row and column of this matrix.
     *
     * @param  row     the row index, from 0 inclusive to {@link #getNumRow()} exclusive.
     * @param  column  the column index, from 0 inclusive to {@link #getNumCol()} exclusive.
     * @return the current value at the given row and column.
     * @throws IndexOutOfBoundsException if the specified row or column is out of bounds.
     */
    @Override
    default double getElement(int row, int column) {
        final Number value = getElementOrNull(row, column);
        return (value != null) ? value.doubleValue() : 0;
    }

    /**
     * Modifies the value at the specified row and column of this matrix.
     * The default implementation assumes that the matrix is unmodifiable.
     *
     * @param  row     the row number of the value to set (zero indexed).
     * @param  column  the column number of the value to set (zero indexed).
     * @param  value   the new matrix element value.
     * @throws IndexOutOfBoundsException if the specified row or column is out of bounds.
     * @throws UnsupportedOperationException if this matrix is unmodifiable.
     */
    @Override
    default void setElement(int row, int column, double value) {
        throw new UnsupportedOperationException(Errors.format(Errors.Keys.UnmodifiableObject_1, getClass()));
    }
}
