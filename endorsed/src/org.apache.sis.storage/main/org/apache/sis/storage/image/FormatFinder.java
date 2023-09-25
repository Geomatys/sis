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
package org.apache.sis.storage.image;

import java.util.Map;
import java.util.LinkedHashMap;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.IIOException;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.spi.ImageWriterSpi;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.FileImageOutputStream;
import java.awt.image.RenderedImage;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.io.stream.InternalOptionKey;
import org.apache.sis.io.stream.IOUtilities;
import org.apache.sis.util.Workaround;


/**
 * Helper class for finding the {@link ImageReader} or {@link ImageWriter} instance to use.
 * This is a temporary object used only at {@link WorldFileStore} construction time.
 * It also helps to choose which {@link WorldFileStore} subclass to instantiate.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.4
 * @since   1.2
 */
final class FormatFinder implements AutoCloseable {
    /**
     * The factory that created this {@code DataStore} instance, or {@code null} if unspecified.
     */
    final WorldFileStoreProvider provider;

    /**
     * Information about the storage (URL, stream, <i>etc</i>).
     */
    final StorageConnector connector;

    /**
     * The {@link #connector} object to keep open if we successfully created a {@link WorldFileStore}.
     * This is often the same object than {@link #storage} but may be different if an {@link ImageInputStream}
     * has been created from the storage object.
     *
     * <p>This value is {@code null} until successful instantiation of image reader or writer.
     * A null value means to close everything, which is the desired behavior in case of failure.</p>
     */
    Object keepOpen;

    /**
     * The file, URL or stream where to read or write the image.
     * If the {@linkplain StorageConnector#getStorage() user-specified storage} was an {@link ImageReader}
     * or {@link ImageWriter}, then the value stored in this field is the input/output of the reader/writer.
     */
    final Object storage;

    /**
     * The image reader if specified or created by this {@code FormatFinder}, or {@code null}.
     */
    private ImageReader reader;

    /**
     * The image writer if specified or created by this {@code FormatFinder}, or {@code null}.
     */
    private ImageWriter writer;

    /**
     * Whether we already made an attempt to find the image reader or writer using {@link ImageIO} registry.
     */
    private boolean readerLookupDone, writerLookupDone;

    /**
     * {@code true} if the {@linkplain #storage} seems to be writable.
     */
    final boolean isWritable;

    /**
     * {@code true} if the storage should be open is write mode instead of read mode.
     * This is {@code true} if the file does not exist or the file is empty.
     */
    final boolean openAsWriter;

    /**
     * {@code true} if the file is known to be empty, or {@code false} in case of doubt.
     */
    final boolean fileIsEmpty;

    /**
     * The filename extension (may be an empty string), or {@code null} if unknown.
     * It does not include the leading dot.
     */
    final String suffix;

    /**
     * Name of the preferred format, or {@code null} if none.
     */
    private final String preferredFormat;

    /**
     * Creates a new format finder.
     *
     * @param  provider   the factory that created this {@code DataStore} instance, or {@code null} if unspecified.
     * @param  connector  information about the storage (URL, stream, <i>etc</i>).
     */
    FormatFinder(final WorldFileStoreProvider provider, final StorageConnector connector)
            throws DataStoreException, IOException
    {
        this.provider  = provider;
        this.connector = connector;
        Object storage = connector.getStorage();
        if (storage instanceof ImageReader) {
            reader  = (ImageReader) storage;
            storage = reader.getInput();
            readerLookupDone = true;
        } else if (storage instanceof ImageWriter) {
            writer  = (ImageWriter) storage;
            storage = writer.getOutput();
            writerLookupDone = true;
        }
        this.storage = storage;
        this.suffix  = IOUtilities.extension(storage);
        final var filter = connector.getOption(InternalOptionKey.PREFERRED_PROVIDERS);
        preferredFormat = filter instanceof DataStoreFilter ? ((DataStoreFilter) filter).preferred : null;
        /*
         * Detect if the image can be opened in read/write mode.
         * If not, it will be opened in read-only mode.
         */
        if (writer != null) {
            isWritable   = true;
            openAsWriter = true;
            fileIsEmpty  = false;
        } else if (reader != null) {
            isWritable   = (reader.getInput() instanceof DataOutput);         // Parent of ImageOutputStream.
            openAsWriter = false;
            fileIsEmpty  = false;
        } else {
            isWritable = WorldFileStoreProvider.isWritable(connector);
            if (isWritable) {
                final Path path = connector.getStorageAs(Path.class);
                if (path != null) {
                    fileIsEmpty  = !Files.exists(path) || Files.size(path) == 0;
                    openAsWriter = fileIsEmpty;
                    return;
                }
            }
            openAsWriter = IOUtilities.isWriteOnly(storage);
            fileIsEmpty  = openAsWriter;
        }
    }

    /**
     * Returns the name of the format.
     *
     * @return name of the format, or {@code null} if unknown.
     */
    final String[] getFormatName() throws DataStoreException, IOException {
        if (openAsWriter) {
            final ImageWriter writer = getOrCreateWriter(null);
            if (writer != null) {
                final ImageWriterSpi spi = writer.getOriginatingProvider();
                if (spi != null) {
                    return spi.getFormatNames();
                }
            }
        } else {
            final ImageReader reader = getOrCreateReader();
            if (reader != null) {
                final ImageReaderSpi spi = reader.getOriginatingProvider();
                if (spi != null) {
                    return spi.getFormatNames();
                }
            }
        }
        return null;
    }

    /**
     * Returns the user-specified reader or searches for a reader that claim to be able to read the storage input.
     * This method tries first the readers associated to the file suffix. If no reader is found, then this method
     * tries all other readers.
     *
     * @return the reader, or {@code null} if none could be found.
     */
    final ImageReader getOrCreateReader() throws DataStoreException, IOException {
        if (!readerLookupDone) {
            readerLookupDone = true;
            final Map<ImageReaderSpi,Boolean> deferred = new LinkedHashMap<>();
            if (preferredFormat != null) {
                reader = FormatFilter.NAME.createReader(preferredFormat, this, deferred);
            }
            if (reader == null && suffix != null) {
                reader = FormatFilter.SUFFIX.createReader(suffix, this, deferred);
            }
            if (reader == null) {
                reader = FormatFilter.SUFFIX.createReader(null, this, deferred);
                if (reader == null) {
                    /*
                     * If no reader has been found, maybe `StorageConnector` has not been able to create
                     * an `ImageInputStream`. It may happen if the storage object is of unknown type.
                     * Check if it is the case, then try all providers that we couldn't try because of that.
                     */
                    ImageInputStream stream = null;
                    for (final Map.Entry<ImageReaderSpi,Boolean> entry : deferred.entrySet()) {
                        if (entry.getValue()) {
                            if (stream == null) try {
                                if (isWritable) {
                                    // ImageOutputStream is both read and write.
                                    stream = ImageIO.createImageOutputStream(storage);
                                }
                                if (stream == null) {
                                    stream = ImageIO.createImageInputStream(storage);
                                    if (stream == null) break;
                                }
                            } catch (IIOException e) {
                                throw unwrap(e);
                            }
                            final ImageReaderSpi p = entry.getKey();
                            if (p.canDecodeInput(stream)) {
                                reader = p.createReaderInstance();
                                reader.setInput(stream);
                                keepOpen = storage;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return reader;
    }

    /**
     * Returns the user-specified writer or searches for a writer for the file suffix.
     *
     * @param  image  the image to write, or {@code null} if unknown.
     * @return the writer, or {@code null} if none could be found.
     */
    final ImageWriter getOrCreateWriter(final RenderedImage image) throws DataStoreException, IOException {
        if (!writerLookupDone) {
            writerLookupDone = true;
            final Map<ImageWriterSpi,Boolean> deferred = new LinkedHashMap<>();
            if (preferredFormat != null) {
                writer = FormatFilter.NAME.createWriter(preferredFormat, this, image, deferred);
            }
            if (writer == null && suffix != null) {
                writer = FormatFilter.SUFFIX.createWriter(suffix, this, image, deferred);
            }
            if (writer == null) {
                writer = FormatFilter.SUFFIX.createWriter(null, this, image, deferred);
                if (writer == null) {
                    ImageOutputStream stream = null;
                    for (final Map.Entry<ImageWriterSpi,Boolean> entry : deferred.entrySet()) {
                        if (entry.getValue()) {
                            if (stream == null) {
                                final File file = connector.getStorageAs(File.class);
                                if (file != null) {
                                    stream = new FileImageOutputStream(file);
                                } else try {
                                    stream = ImageIO.createImageOutputStream(storage);
                                    if (stream == null) break;
                                } catch (IIOException e) {
                                    throw unwrap(e);
                                }
                            }
                            final ImageWriterSpi p = entry.getKey();
                            writer = p.createWriterInstance();
                            writer.setOutput(stream);
                            keepOpen = storage;
                            break;
                        }
                    }
                }
            }
        }
        return writer;
    }

    /**
     * Returns the cause of given exception if it exists, or the exception itself otherwise.
     * This method is invoked in the {@code catch} block of a {@code try} block invoking
     * {@link ImageIO#createImageInputStream(Object)} or
     * {@link ImageIO#createImageOutputStream(Object)}.
     *
     * <h4>Rational</h4>
     * As of Java 18, above-cited methods systematically catch all {@link IOException}s and wrap
     * them in an {@link IIOException} with <cite>"Cannot create cache file!"</cite> error message.
     * This is conform to Image I/O specification but misleading if the stream provider throws an
     * {@link IOException} for another reason. Even when the failure is really caused by a problem
     * with cache file, we want to propagate the original exception to user because its message
     * may tell that there is no space left on device or no write permission.
     *
     * @see org.apache.sis.storage.StorageConnector#unwrap(IIOException)
     */
    @Workaround(library = "JDK", version = "18")
    private static IOException unwrap(final IIOException e) {
        final Throwable cause = e.getCause();
        return (cause instanceof IOException) ? (IOException) cause : e;
    }

    /**
     * Closes all unused resources. Keep open only the find of objects needed by the image reader or writer.
     * This method must be invoked after by {@link WorldFileStore} construction.
     */
    @Override
    public final void close() throws DataStoreException {
        connector.closeAllExcept(keepOpen);
    }
}
