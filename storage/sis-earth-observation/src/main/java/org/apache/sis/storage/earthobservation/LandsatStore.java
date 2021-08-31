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
package org.apache.sis.storage.earthobservation;

import java.io.Reader;
import java.io.BufferedReader;
import java.io.LineNumberReader;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.net.URI;
import java.util.Optional;
import org.opengis.metadata.Metadata;
import org.opengis.util.GenericName;
import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterValueGroup;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStoreClosedException;
import org.apache.sis.storage.DataStoreReferencingException;
import org.apache.sis.storage.UnsupportedStorageException;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.event.StoreEvent;
import org.apache.sis.storage.event.StoreListener;
import org.apache.sis.storage.event.WarningEvent;
import org.apache.sis.internal.storage.URIDataStore;
import org.apache.sis.setup.OptionKey;


/**
 * Parses Landsat metadata as {@linkplain org.apache.sis.metadata.iso.DefaultMetadata ISO 19115 Metadata} object.
 * Landsat data are distributed as a collection of TIFF files, together with a single
 * text file like below:
 *
 * {@preformat text
 * GROUP = L1_METADATA_FILE
 *   GROUP = METADATA_FILE_INFO
 *     ORIGIN = "Image courtesy of the U.S. Geological Survey"
 *     REQUEST_ID = "0501403126384_00011"
 *     LANDSAT_SCENE_ID = "LC81230522014071LGN00"
 *     FILE_DATE = 2014-03-12T06:06:35Z
 *     STATION_ID = "LGN"
 *     PROCESSING_SOFTWARE_VERSION = "LPGS_2.3.0"
 *   END_GROUP = METADATA_FILE_INFO
 *   GROUP = PRODUCT_METADATA
 *     DATA_TYPE = "L1T"
 *     ELEVATION_SOURCE = "GLS2000"
 *     OUTPUT_FORMAT = "GEOTIFF"
 *     SPACECRAFT_ID = "LANDSAT_8"
 *     SENSOR_ID = "OLI_TIRS"
 *     etc...
 * }
 *
 * This class reads the content from the given input until the first occurrence of the {@code END} keyword.
 * Lines beginning with the {@code #} character (ignoring spaces) are treated as comment lines and ignored.
 *
 * @author  Thi Phuong Hao Nguyen (VNSC)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   0.8
 * @module
 */
public class LandsatStore extends DataStore {
    /**
     * The reader, or {@code null} if closed.
     */
    private Reader source;

    /**
     * The {@link LandsatStoreProvider#LOCATION} parameter value, or {@code null} if none.
     */
    private final URI location;

    /**
     * The object returned by {@link #getMetadata()}, created when first needed and cached.
     */
    private Metadata metadata;

    /**
     * The identifier, cached when first requested.
     */
    private GenericName identifier;

    /**
     * Creates a new Landsat store from the given file, URL, stream or character reader.
     * This constructor invokes {@link StorageConnector#closeAllExcept(Object)},
     * keeping open only the needed resource.
     *
     * @param  provider   the factory that created this {@code DataStore} instance, or {@code null} if unspecified.
     * @param  connector  information about the storage (URL, stream, reader instance, <i>etc</i>).
     * @throws DataStoreException if an error occurred while opening the Landsat file.
     */
    public LandsatStore(final LandsatStoreProvider provider, final StorageConnector connector) throws DataStoreException {
        super(provider, connector);
        location = connector.getStorageAs(URI.class);
        source = connector.getStorageAs(Reader.class);
        connector.closeAllExcept(source);
        if (source == null) {
            throw new UnsupportedStorageException(super.getLocale(), LandsatStoreProvider.NAME,
                    connector.getStorage(), connector.getOption(OptionKey.OPEN_OPTIONS));
        }
    }

    /**
     * Returns the parameters used to open this Landsat data store.
     * The parameters are described by {@link LandsatStoreProvider#getOpenParameters()} and contains at least
     * a parameter named {@value org.apache.sis.storage.DataStoreProvider#LOCATION} with a {@link URI} value.
     * The return value may be empty if the storage input can not be described by a URI
     * (for example a Landsat file reading directly from a {@link java.nio.channels.ReadableByteChannel}).
     *
     * @return parameters used for opening this data store.
     *
     * @since 0.8
     */
    @Override
    public Optional<ParameterValueGroup> getOpenParameters() {
        return Optional.ofNullable(URIDataStore.parameters(provider, location));
    }

    /**
     * Returns the value associated to {@code LANDSAT_SCENE_ID} in the Landsat metadata file.
     * This value is fetched from
     * <code>{@linkplain #getMetadata()}/​identificationInfo/​citation/​identifier</code>.
     *
     * @return the identifier fetched from metadata, or {@code null} if none.
     * @throws DataStoreException if an error occurred while reading the metadata.
     *
     * @since 1.0
     */
    @Override
    public synchronized Optional<GenericName> getIdentifier() throws DataStoreException {
        if (identifier == null) {
            identifier = super.getIdentifier().orElse(null);
        }
        return Optional.ofNullable(identifier);
    }

    /**
     * Returns information about the dataset as a whole. The returned metadata object can contain information
     * such as the spatiotemporal extent of the dataset, contact information about the creator or distributor,
     * data quality, usage constraints and more.
     *
     * @return information about the dataset.
     * @throws DataStoreException if an error occurred while reading the metadata.
     */
    @Override
    public synchronized Metadata getMetadata() throws DataStoreException {
        if (metadata == null) {
            if (source == null) {
                throw new DataStoreClosedException(getLocale(), LandsatStoreProvider.NAME, StandardOpenOption.READ);
            }
            try (BufferedReader reader = (source instanceof BufferedReader) ? (BufferedReader) source : new LineNumberReader(source)) {
                source = null;      // Will be closed at the end of this try-finally block.
                final LandsatReader parser = new LandsatReader(getDisplayName(), listeners);
                parser.read(reader);
                metadata = parser.getMetadata();
            } catch (IOException e) {
                throw new DataStoreException(e);
            } catch (FactoryException e) {
                throw new DataStoreReferencingException(e);
            }
        }
        return metadata;
    }

    /**
     * Registers a listener to notify when the specified kind of event occurs in this data store.
     * The current implementation of this data store can emit only {@link WarningEvent}s;
     * any listener specified for another kind of events will be ignored.
     */
    @Override
    public <T extends StoreEvent> void addListener(Class<T> eventType, StoreListener<? super T> listener) {
        // If an argument is null, we let the parent class throws (indirectly) NullArgumentException.
        if (listener == null || eventType == null || eventType.isAssignableFrom(WarningEvent.class)) {
            super.addListener(eventType, listener);
        }
    }

    /**
     * Closes this Landsat store and releases any underlying resources.
     *
     * @throws DataStoreException if an error occurred while closing the Landsat file.
     */
    @Override
    public synchronized void close() throws DataStoreException {
        metadata = null;
    }
}
