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
package org.apache.sis.internal.sql.feature;

import java.util.List;
import java.util.Collections;
import org.apache.sis.internal.util.UnmodifiableArrayList;


/**
 * Represents SQL primary key constraint. Main information is columns composing the key.
 *
 * <h2>Implementation notes</h2>
 * For now, only list of columns composing the key are returned. However, in the future it would be possible
 * to add other information, as a value type to describe how to expose primary key value.
 *
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
abstract class PrimaryKey {
    /**
     * For sub-class constructors only.
     */
    PrimaryKey() {
    }

    /**
     * Creates a new key for the given columns, or returns {@code null} if none.
     *
     * @param  columns  the columns composing the primary key. May be empty.
     * @return the primary key, or {@code null} if the given list is empty.
     */
    static PrimaryKey create(final List<String> columns) {
        switch (columns.size()) {
            case 0:  return null;
            case 1:  return new Single(columns.get(0));
            default: return new Composite(columns);
        }
    }

    /**
     * Returns the list of column names composing the key.
     * Shall never be null nor empty.
     *
     * @return column names composing the key. Contains at least one element.
     */
    public abstract List<String> getColumns();

    /**
     * A primary key composed of exactly one column.
     */
    private static final class Single extends PrimaryKey {
        /** The single column name. */
        private final String column;

        /** Creates a new primary key composed of the given column. */
        Single(final String column) {
            this.column = column;
        }

        /** Returns the single column composing this primary key. */
        @Override
        public List<String> getColumns() {
            return Collections.singletonList(column);
        }
    }

    /**
     * A primary key composed of two or more columns.
     */
    private static final class Composite extends PrimaryKey {
        /** Name of columns composing the primary key. */
        private final List<String> columns;

        /** Creates a new primary key composed of the given columns. */
        Composite(final List<String> columns) {
            this.columns = UnmodifiableArrayList.wrap(columns.toArray(new String[columns.size()]));
        }

        /** Returns all columns composing this primary key. */
        @Override
        @SuppressWarnings("ReturnOfCollectionOrArrayField")
        public List<String> getColumns() {
            return columns;
        }
    }
}
