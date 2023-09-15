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
package org.apache.sis.storage;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

// Specific to the main branch:
import org.apache.sis.feature.AbstractFeature;
import org.apache.sis.feature.DefaultFeatureType;


/**
 * A {@link FeatureSet} with writing capabilities. {@code WritableFeatureSet} inherits the reading capabilities from
 * its parent and adds the capabilities to {@linkplain #add(Iterator) add}, {@linkplain #removeIf(Predicate) remove}
 * or {@linkplain #replaceIf(Predicate, UnaryOperator) replace} feature instances.
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.0
 * @since   1.0
 */
public interface WritableFeatureSet extends FeatureSet {
    /**
     * Declares or redefines the type of all feature instances in this feature set.
     * In the case of a newly created feature set, this method can be used for defining the type of features
     * to be stored (this is not a required step however). In the case of a feature set which already contains
     * feature instances, this operation may take an undefined amount of time to execute since all features in
     * the set may need to be transformed.
     *
     * <p>Feature sets may restrict the kind of changes that are allowed. An {@link IllegalFeatureTypeException}
     * will be thrown if the given type contains incompatible property changes.</p>
     *
     * @param  newType  new feature type definition (not {@code null}).
     * @throws IllegalFeatureTypeException if the given type is not compatible with the types supported by the store.
     * @throws DataStoreException if another error occurred while changing the feature type.
     */
    void updateType(DefaultFeatureType newType) throws DataStoreException;

    /**
     * Inserts new feature instances in this {@code FeatureSet}.
     * Any feature already present in this {@link FeatureSet} will remain unmodified.
     * If a {@linkplain AbstractFeature#getProperty feature property} is used as unique identifier, then:
     *
     * <ul>
     *   <li>If a given feature assigns to that property a value already in use, an exception will be thrown.</li>
     *   <li>If given features do not assign value to that property, identifiers should be generated by the data store.</li>
     * </ul>
     *
     * After successful insertion, the new features may appear after the features already present
     * but not necessarily; ordering is {@link DataStore} specific.
     *
     * <h4>API note</h4>
     * This method expects an {@link Iterator} rather than a {@link Stream} for easing
     * inter-operability with various API. Implementing a custom {@link Iterator} requires less effort
     * than implementing a {@link Stream}. On the other side if the user has a {@link Stream},
     * obtaining an {@link Iterator} can be done by a call to {@link Stream#iterator()}.
     *
     * @param  features feature instances to insert or copy in this {@code FeatureSet}.
     * @throws IllegalFeatureTypeException if a feature given by the iterator is not of the type expected by this {@code FeatureSet}.
     * @throws DataStoreException if another error occurred while storing new features.
     */
    void add(Iterator<? extends AbstractFeature> features) throws DataStoreException;

    /**
     * Removes all feature instances from this {@code FeatureSet} which matches the given predicate.
     *
     * <div class="warning"><b>Possible API change:</b>
     * The {@code boolean} return type may be removed in a future version.
     * It currently exists for compatibility with the {@link java.util.Collection#removeIf(Predicate)} method
     * if an implementation chooses to implements {@code WritableFeatureSet} and {@link java.util.Collection}
     * in same time. But this is not recommended, and the current method signature is a blocker for deferred
     * method execution. Telling if there is any feature to remove requires immediate execution of the filter,
     * while an implementation may want to wait in case the filtering can be combined with other operations
     * such as {@code add(…)} or {@code replaceIf(…)}.
     * See <a href="https://issues.apache.org/jira/browse/SIS-560">SIS-560 on issue tracker</a>.</div>
     *
     * @param  filter  a predicate which returns {@code true} for feature instances to be removed.
     * @return {@code true} if any elements were removed.
     * @throws DataStoreException if an error occurred while removing features.
     */
    boolean removeIf(Predicate<? super AbstractFeature> filter) throws DataStoreException;

    /**
     * Updates all feature instances from this {@code FeatureSet} which match the given predicate.
     * For each {@code Feature} instance matching the given {@link Predicate},
     * the <code>{@linkplain UnaryOperator#apply UnaryOperator.apply(Feature)}</code> method will be invoked.
     * {@code UnaryOperator}s are free to modify the given {@code Feature} <i>in-place</i>
     * or to return a different feature instance. Two behaviors are possible:
     *
     * <ul>
     *   <li>If the operator returns a non-null {@code Feature}, then the modified feature is stored
     *       in replacement of the previous feature (not necessarily at the same location).</li>
     *   <li>If the operator returns {@code null}, then the feature will be removed from the {@code FeatureSet}.</li>
     * </ul>
     *
     * @param  filter   a predicate which returns {@code true} for feature instances to be updated.
     * @param  updater  operation called for each matching {@code Feature} instance.
     * @throws IllegalFeatureTypeException if a feature given by the operator is not of the type expected by this {@code FeatureSet}.
     * @throws DataStoreException if another error occurred while replacing features.
     */
    void replaceIf(Predicate<? super AbstractFeature> filter, UnaryOperator<AbstractFeature> updater) throws DataStoreException;
}