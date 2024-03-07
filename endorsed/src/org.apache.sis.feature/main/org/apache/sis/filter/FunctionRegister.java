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
package org.apache.sis.filter;

import java.util.Collection;


/**
 * A factory of {@code org.opengis.filter} functions identified by their names.
 * Each factory can provide an arbitrary number of functions, enumerated by {@link #getNames()}.
 * The {@link org.apache.sis.filter.DefaultFilterFactory#function(String, Expression...)} method
 * delegates to this interface for creating the function implementation for a given name.
 *
 * <p><b>Warning:</b> there is currently no mechanism for avoiding name collision.
 * It is implementer responsibility to keep trace of the whole universe of functions and avoid collision.
 * Checks against name collisions may be added in a future version.</p>
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.5
 * @since   1.5
 */
public interface FunctionRegister {
    /**
     * Returns the name of the standard or authority defining the functions.
     *
     * @return provider of function definitions.
     */
    String getAuthority();

    /**
     * Returns the names of all functions that this factory can create.
     * It is currently implementer responsibility to ensure that there are no name collisions with
     * functions provided by other factories (this problem may be improved in future SIS release).
     *
     * @return set of supported function names.
     */
    Collection<String> getNames();

    /**
     * Creates a new function of the given name with given parameters.
     *
     * @param  <R>         the type of resources (e.g. {@code Feature}) used as inputs.
     * @param  name        name of the function to create (not null).
     * @param  parameters  function parameters.
     * @return function for the given name and parameters.
     * @throws IllegalArgumentException if function name is unknown or some parameters are illegal.
     */
    <R> Expression<R,?> create(String name, Expression<R,?>[] parameters) throws IllegalArgumentException;
}
