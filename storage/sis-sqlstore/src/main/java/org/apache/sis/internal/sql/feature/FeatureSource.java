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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.apache.sis.storage.DataStoreException;

// Branch-dependent imports
import org.opengis.feature.Feature;


/**
 * A component capable of loading data from an SQL connection.
 * Used for extracting a subset of a {@code FeatureSet} from a SQL database.
 *
 * @author  Alexis Manin (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
abstract class FeatureSource {
    /**
     * Creates a new connector.
     */
    FeatureSource() {
    }

    /**
     * Initiates the loading of data through an existing connection.
     * Implementations are encouraged to perform lazy loading of feature instances.
     *
     * @param  connection  the database connection to use for data loading. It is caller's responsibility to close
     *                     the connection, and it should not be done before stream terminal operation is completed.
     * @return features loaded from given connection. Implementations are encouraged to perform lazy loading.
     * @throws SQLException if an error occurs while exchanging information with the database.
     * @throws DataStoreException if a data model dependent error occurs.
     */
    public abstract Stream<Feature> stream(Connection connection) throws SQLException, DataStoreException;

    /**
     * Provides a SQL statement for "simple" feature data or for a summary of available features.
     * The returned statement is valid SQL but does not need to provide all properties contained in the
     * {@linkplain #stream stream of features}. For example if a property is an association to another feature,
     * the SQL statement may provide only the foreigner key values, not an inner join to the other feature table.
     *
     * @param  count  {@code true} for a query counting the number of features, or
     *                {@code false} for a query listing the feature instances.
     * @return SQL statement for main feature data or a summary of those data, or
     *         {@code null} if this {@code FeatureSource} can not provide a SQL query.
     */
    public abstract String overviewStatement(boolean count);
}
