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
package org.apache.sis.io.stream;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.sis.setup.OptionKey;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.DataStoreProvider;


/**
 * {@link StorageConnector} options not part of public API.
 * Some of those options may move to public API in the future if useful.
 *
 * @author  Martin Desruisseaux (Geomatys)
 *
 * @param  <T>  the type of option values.
 */
public final class InternalOptionKey<T> extends OptionKey<T> {
    /**
     * For cross-version compatibility.
     */
    private static final long serialVersionUID = 1786137598411493790L;

    /**
     * A filter for trying preferred data stores first. A typical usage is for selecting data
     * store providers based on their {@linkplain DataStoreProvider#getShortName() format name}.
     */
    @SuppressWarnings("unchecked")
    public static final InternalOptionKey<Predicate<DataStoreProvider>> PREFERRED_PROVIDERS =
            (InternalOptionKey) new InternalOptionKey<>("PREFERRED_PROVIDERS", Predicate.class);

    /**
     * Wraps readable or writable channels on creation. Wrappers can be used for example
     * in order to listen to read events or for transforming bytes on the fly.
     */
    @SuppressWarnings("unchecked")
    public static final InternalOptionKey<UnaryOperator<ChannelFactory>> CHANNEL_FACTORY_WRAPPER =
            (InternalOptionKey) new InternalOptionKey<>("READ_CHANNEL_WRAPPER", UnaryOperator.class);

    /**
     * Creates a new key of the given name.
     *
     * @param name  the key name.
     * @param type  the type of values.
     */
    public InternalOptionKey(final String name, final Class<T> type) {
        super(name, type);
    }
}
