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
package org.apache.sis.internal.storage.image;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.io.IOException;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import org.opengis.metadata.Metadata;
import org.opengis.metadata.maintenance.ScopeCode;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.TransformException;
import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.storage.Resource;
import org.apache.sis.storage.Aggregate;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.GridCoverageResource;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStoreClosedException;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreReferencingException;
import org.apache.sis.storage.ReadOnlyStorageException;
import org.apache.sis.storage.UnsupportedStorageException;
import org.apache.sis.internal.storage.Resources;
import org.apache.sis.internal.storage.PRJDataStore;
import org.apache.sis.internal.storage.io.IOUtilities;
import org.apache.sis.internal.referencing.j2d.AffineTransform2D;
import org.apache.sis.internal.storage.MetadataBuilder;
import org.apache.sis.internal.util.ListOfUnknownSize;
import org.apache.sis.metadata.sql.MetadataStoreException;
import org.apache.sis.util.collection.BackingStoreException;
import org.apache.sis.util.resources.Errors;
import org.apache.sis.util.CharSequences;
import org.apache.sis.util.ArraysExt;
import org.apache.sis.setup.OptionKey;


/**
 * A data store which creates grid coverages from Image I/O readers using <cite>World File</cite> convention.
 * Georeferencing is defined by two auxiliary files having the same name than the image file but different suffixes:
 *
 * <ul class="verbose">
 *   <li>A text file containing the coefficients of the affine transform mapping pixel coordinates to geodesic coordinates.
 *     The reader expects one coefficient per line, in the same order than the order expected by the
 *     {@link java.awt.geom.AffineTransform#AffineTransform(double[]) AffineTransform(double[])} constructor, which is
 *     <var>scaleX</var>, <var>shearY</var>, <var>shearX</var>, <var>scaleY</var>, <var>translateX</var>, <var>translateY</var>.
 *     The reader looks for a file having the following suffixes, in preference order:
 *     <ol>
 *       <li>The first letter of the image file extension, followed by the last letter of
 *         the image file extension, followed by {@code 'w'}. Example: {@code "tfw"} for
 *         {@code "tiff"} images, and {@code "jgw"} for {@code "jpeg"} images.</li>
 *       <li>The extension of the image file with a {@code 'w'} appended.</li>
 *       <li>The {@code "wld"} extension.</li>
 *     </ol>
 *   </li>
 *   <li>A text file containing the <cite>Coordinate Reference System</cite> (CRS) definition
 *     in <cite>Well Known Text</cite> (WKT) syntax.
 *     The reader looks for a file having the {@code ".prj"} extension.</li>
 * </ul>
 *
 * Every auxiliary text file are expected to be encoded in UTF-8
 * and every numbers are expected to be formatted in US locale.
 *
 * <h2>Type of input objects</h2>
 * The {@link StorageConnector} input should be an instance of the following types:
 * {@link java.nio.file.Path}, {@link java.io.File}, {@link java.net.URL} or {@link java.net.URI}.
 * Other types such as {@link ImageInputStream} are also accepted but in those cases the auxiliary files can not be read.
 * For any input of unknown type, this data store first checks if an {@link ImageReader} accepts the input type directly.
 * If none is found, this data store tries to {@linkplain ImageIO#createImageInputStream(Object) create an input stream}
 * from the input object.
 *
 * <p>The storage input object may also be an {@link ImageReader} instance ready for use
 * (i.e. with its {@linkplain ImageReader#setInput(Object) input set} to a non-null value).
 * In that case, this data store will use the given image reader as-is.
 * The image reader will be {@linkplain ImageReader#dispose() disposed}
 * and its input closed (if {@link AutoCloseable}) when this data store is {@linkplain #close() closed}.</p>
 *
 * <h2>Handling of multi-image files</h2>
 * Because some image formats can store an arbitrary amount of images,
 * this data store is considered as an aggregate with one resource per image.
 * All image should have the same size and all resources will share the same {@link GridGeometry}.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 * @since   1.2
 * @module
 */
class WorldFileStore extends PRJDataStore implements Aggregate {
    /**
     * Image I/O format names (ignoring case) for which we have an entry in the {@code SpatialMetadata} database.
     */
    private static final String[] KNOWN_FORMATS = {
        "PNG"
    };

    /**
     * Index of the main image. This is relevant only with formats capable to store an arbitrary amount of images.
     * Current implementation assumes that the main image is always the first one, but it may become configurable
     * in a future version if useful.
     *
     * @see #width
     * @see #height
     */
    static final int MAIN_IMAGE = 0;

    /**
     * The default World File suffix when it can not be determined from {@link #location}.
     * This is a GDAL convention.
     */
    private static final String DEFAULT_SUFFIX = "wld";

    /**
     * The "cell center" versus "cell corner" interpretation of translation coefficients.
     * The ESRI specification said that the coefficients map to pixel center.
     */
    static final PixelInCell CELL_ANCHOR = PixelInCell.CELL_CENTER;

    /**
     * The filename extension (may be an empty string), or {@code null} if unknown.
     * It does not include the leading dot.
     */
    final String suffix;

    /**
     * The filename extension for the auxiliary "world file".
     * For the TIFF format, this is typically {@code "tfw"}.
     * This is computed as a side-effect of {@link #readWorldFile()}.
     */
    private String suffixWLD;

    /**
     * The image reader, set by the constructor and cleared when the store is closed.
     * May also be null if the store is initially write-only, in which case a reader
     * may be created the first time than an image is read.
     *
     * @see #reader()
     */
    private ImageReader reader;

    /**
     * Width and height of the main image.
     * The {@link #gridGeometry} is assumed valid only for images having this size.
     *
     * @see #MAIN_IMAGE
     * @see #gridGeometry
     */
    private int width, height;

    /**
     * The conversion from pixel center to CRS, or {@code null} if none or not yet computed.
     * The grid extent has the size given by {@link #width} and {@link #height}.
     *
     * @see #crs
     * @see #width
     * @see #height
     * @see #getGridGeometry(int)
     */
    private GridGeometry gridGeometry;

    /**
     * All images in this resource, created when first needed.
     * Elements in this list will also be created when first needed.
     *
     * @see #components()
     */
    private Components components;

    /**
     * The metadata object, or {@code null} if not yet created.
     *
     * @see #getMetadata()
     */
    private Metadata metadata;

    /**
     * Identifiers used by a resource. Identifiers must be unique in the data store,
     * so after an identifier has been used it can not be reused anymore even if the
     * resource having that identifier has been removed.
     * Values associated to identifiers tell whether the resource still exist.
     *
     * @see WorldFileResource#getIdentifier()
     */
    final Map<String,Boolean> identifiers;

    /**
     * Creates a new store from the given file, URL or stream.
     *
     * @param  provider   the factory that created this {@code DataStore} instance, or {@code null} if unspecified.
     * @param  connector  information about the storage (URL, stream, <i>etc</i>).
     * @param  readOnly   whether to fail if the channel can not be opened at least in read mode.
     * @throws DataStoreException if an error occurred while opening the stream.
     * @throws IOException if an error occurred while creating the image reader instance.
     */
    WorldFileStore(final WorldFileStoreProvider provider, final StorageConnector connector, final boolean readOnly)
            throws DataStoreException, IOException
    {
        super(provider, connector);
        identifiers = new HashMap<>();
        final Object storage = connector.getStorage();
        if (storage instanceof ImageReader) {
            reader = (ImageReader) storage;
            suffix = IOUtilities.extension(reader.getInput());
            configureReader();
            return;
        }
        if (storage instanceof ImageWriter) {
            suffix = IOUtilities.extension(((ImageWriter) storage).getOutput());
            return;
        }
        suffix = IOUtilities.extension(storage);
        if (!(readOnly || fileExists(connector))) {
            /*
             * If the store is opened in read-write mode, create the image reader only
             * if the file exists and is non-empty. Otherwise we let `reader` to null
             * and the caller will create an image writer instead.
             */
            return;
        }
        /*
         * Search for a reader that claim to be able to read the storage input.
         * First we try readers associated to the file suffix. If no reader is
         * found, we try all other readers.
         */
        final Map<ImageReaderSpi,Boolean> deferred = new LinkedHashMap<>();
        if (suffix != null) {
            reader = FormatFilter.SUFFIX.createReader(suffix, connector, deferred);
        }
        if (reader == null) {
            reader = FormatFilter.SUFFIX.createReader(null, connector, deferred);
fallback:   if (reader == null) {
                /*
                 * If no reader has been found, maybe `StorageConnector` has not been able to create
                 * an `ImageInputStream`. It may happen if the storage object is of unknown type.
                 * Check if it is the case, then try all providers that we couldn't try because of that.
                 */
                ImageInputStream stream = null;
                for (final Map.Entry<ImageReaderSpi,Boolean> entry : deferred.entrySet()) {
                    if (entry.getValue()) {
                        if (stream == null) {
                            if (!readOnly) {
                                // ImageOutputStream is both read and write.
                                stream = ImageIO.createImageOutputStream(storage);
                            }
                            if (stream == null) {
                                stream = ImageIO.createImageInputStream(storage);
                                if (stream == null) break;
                            }
                        }
                        final ImageReaderSpi p = entry.getKey();
                        if (p.canDecodeInput(stream)) {
                            connector.closeAllExcept(storage);
                            reader = p.createReaderInstance();
                            reader.setInput(stream);
                            break fallback;
                        }
                    }
                }
                throw new UnsupportedStorageException(super.getLocale(), WorldFileStoreProvider.NAME,
                            storage, connector.getOption(OptionKey.OPEN_OPTIONS));
            }
        }
        configureReader();
        /*
         * Do not invoke any method that may cause the image reader to start reading the stream,
         * because the `WritableStore` subclass will want to save the initial stream position.
         */
    }

    /**
     * Sets the locale to use for warning messages, if supported. If the reader
     * does not support the locale, the reader's default locale will be used.
     */
    private void configureReader() {
        try {
            reader.setLocale(listeners.getLocale());
        } catch (IllegalArgumentException e) {
            // Ignore
        }
        reader.addIIOReadWarningListener(new WarningListener(listeners));
    }

    /**
     * Returns {@code true} if the image file exists and is non-empty.
     * This is used for checking if an {@link ImageReader} should be created.
     * If the file is going to be truncated, then it is considered already empty.
     *
     * @param  connector  the connector to use for opening the file.
     * @return whether the image file exists and is non-empty.
     */
    private boolean fileExists(final StorageConnector connector) throws DataStoreException, IOException {
        if (!ArraysExt.contains(connector.getOption(OptionKey.OPEN_OPTIONS), StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Path path : super.getComponentFiles()) {
                if (Files.isRegularFile(path) && Files.size(path) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the preferred suffix for the auxiliary world file. For TIFF images, this is {@code "tfw"}.
     * This method tries to use the same case (lower-case or upper-case) than the suffix of the main file.
     */
    private String getWorldFileSuffix() {
        if (suffix != null) {
            final int length = suffix.length();
            if (suffix.codePointCount(0, length) >= 2) {
                boolean lower = true;
                for (int i = length; i > 0;) {
                    final int c = suffix.codePointBefore(i);
                    lower =  Character.isLowerCase(c); if ( lower) break;
                    lower = !Character.isUpperCase(c); if (!lower) break;
                    i -= Character.charCount(c);
                }
                // If the case can not be determined, `lower` will default to `true`.
                return new StringBuilder(3)
                        .appendCodePoint(suffix.codePointAt(0))
                        .appendCodePoint(suffix.codePointBefore(length))
                        .append(lower ? 'w' : 'W').toString();
            }
        }
        return DEFAULT_SUFFIX;
    }

    /**
     * Reads the "World file" by searching for an auxiliary file with a suffix inferred from
     * the suffix of the main file. This method tries suffixes with the following conventions,
     * in preference order.
     *
     * <ol>
     *   <li>First letter of main file suffix, followed by last letter, followed by {@code 'w'}.</li>
     *   <li>Full suffix of the main file followed by {@code 'w'}.</li>
     *   <li>{@value #DEFAULT_SUFFIX}.</li>
     * </ol>
     *
     * @return the "World file" content as an affine transform, or {@code null} if none was found.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the auxiliary file content can not be parsed.
     */
    private AffineTransform2D readWorldFile() throws IOException, DataStoreException {
        IOException warning = null;
        final String preferred = getWorldFileSuffix();
loop:   for (int convention=0;; convention++) {
            final String wld;
            switch (convention) {
                default: break loop;
                case 0:  wld = preferred;      break;       // First file suffix to search.
                case 2:  wld = DEFAULT_SUFFIX; break;       // File suffix to search in last resort.
                case 1: {
                    if (preferred.equals(DEFAULT_SUFFIX)) break loop;
                    wld = suffix + preferred.charAt(preferred.length() - 1);
                    break;
                }
            }
            try {
                return readWorldFile(wld);
            } catch (NoSuchFileException | FileNotFoundException e) {
                if (warning == null) {
                    warning = e;
                } else {
                    warning.addSuppressed(e);
                }
            }
        }
        if (warning != null) {
            listeners.warning(resources().getString(Resources.Keys.CanNotReadAuxiliaryFile_1, preferred), warning);
        }
        return null;
    }

    /**
     * Reads the "World file" by parsing an auxiliary file with the given suffix.
     *
     * @param  wld  suffix of the auxiliary file.
     * @return the "World file" content as an affine transform.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the file content can not be parsed.
     */
    private AffineTransform2D readWorldFile(final String wld) throws IOException, DataStoreException {
        final AuxiliaryContent content  = readAuxiliaryFile(wld, encoding);
        final String           filename = content.getFilename();
        final CharSequence[]   lines    = CharSequences.splitOnEOL(readAuxiliaryFile(wld, encoding));
        final int              expected = 6;        // Expected number of elements.
        int                    count    = 0;        // Actual number of elements.
        final double[]         elements = new double[expected];
        for (int i=0; i<expected; i++) {
            final String line = lines[i].toString().trim();
            if (!line.isEmpty() && line.charAt(0) != '#') {
                if (count >= expected) {
                    throw new DataStoreContentException(errors().getString(Errors.Keys.TooManyOccurrences_2, expected, "coefficient"));
                }
                try {
                    elements[count++] = Double.parseDouble(line);
                } catch (NumberFormatException e) {
                    throw new DataStoreContentException(errors().getString(Errors.Keys.ErrorInFileAtLine_2, filename, i), e);
                }
            }
        }
        if (count != expected) {
            throw new EOFException(errors().getString(Errors.Keys.UnexpectedEndOfFile_1, filename));
        }
        if (filename != null) {
            final int s = filename.lastIndexOf('.');
            if (s >= 0) {
                suffixWLD = filename.substring(s+1);
            }
        }
        return new AffineTransform2D(elements);
    }

    /**
     * Returns the localized resources for producing warnings or error messages.
     */
    final Resources resources() {
        return Resources.forLocale(listeners.getLocale());
    }

    /**
     * Returns the localized resources for producing error messages.
     */
    private Errors errors() {
        return Errors.getResources(listeners.getLocale());
    }

    /**
     * Returns paths to the main file together with auxiliary files.
     *
     * @return paths to the main file and auxiliary files, or an empty array if unknown.
     * @throws DataStoreException if the URI can not be converted to a {@link Path}.
     */
    @Override
    public final synchronized Path[] getComponentFiles() throws DataStoreException {
        if (suffixWLD == null) try {
            getGridGeometry(MAIN_IMAGE);                // Will compute `suffixWLD` as a side effect.
        } catch (IOException e) {
            throw new DataStoreException(e);
        }
        return listComponentFiles(suffixWLD, PRJ);      // `suffixWLD` still null if file was not found.
    }

    /**
     * Gets the grid geometry for image at the given index.
     * This method should be invoked only once per image, and the result cached.
     *
     * @param  index  index of the image for which to read the grid geometry.
     * @return grid geometry of the image at the given index.
     * @throws IndexOutOfBoundsException if the image index is out of bounds.
     * @throws IOException if an I/O error occurred.
     * @throws DataStoreException if the {@code *.prj} or {@code *.tfw} auxiliary file content can not be parsed.
     */
    final GridGeometry getGridGeometry(final int index) throws IOException, DataStoreException {
        assert Thread.holdsLock(this);
        final ImageReader reader = reader();
        if (gridGeometry == null) {
            final AffineTransform2D gridToCRS;
            width     = reader.getWidth (MAIN_IMAGE);
            height    = reader.getHeight(MAIN_IMAGE);
            gridToCRS = readWorldFile();
            readPRJ();
            gridGeometry = new GridGeometry(new GridExtent(width, height), CELL_ANCHOR, gridToCRS, crs);
        }
        if (index != MAIN_IMAGE) {
            final int w = reader.getWidth (index);
            final int h = reader.getHeight(index);
            if (w != width || h != height) {
                // Can not use `gridToCRS` and `crs` because they may not apply.
                return new GridGeometry(new GridExtent(w, h), CELL_ANCHOR, null, null);
            }
        }
        return gridGeometry;
    }

    /**
     * Sets the store-wide grid geometry when a new coverage is written. The {@link WritableStore} implementation
     * is responsible for making sure that the new grid geometry is compatible with preexisting grid geometry.
     *
     * @param  index  index of the image for which to set the grid geometry.
     * @param  gg     the new grid geometry.
     * @return suffix of the "world file", or {@code null} if the image can not be written.
     */
    String setGridGeometry(final int index, final GridGeometry gg) throws IOException, DataStoreException {
        if (index != MAIN_IMAGE) {
            return null;
        }
        final GridExtent extent = gg.getExtent();
        final int w = Math.toIntExact(extent.getSize(WorldFileResource.X_DIMENSION));
        final int h = Math.toIntExact(extent.getSize(WorldFileResource.Y_DIMENSION));
        final String s = (suffixWLD != null) ? suffixWLD : getWorldFileSuffix();
        crs = gg.isDefined(GridGeometry.CRS) ? gg.getCoordinateReferenceSystem() : null;
        gridGeometry = gg;                  // Set only after success of all the above.
        width        = w;
        height       = h;
        suffixWLD    = s;
        return s;
    }

    /**
     * Returns information about the data store as a whole.
     */
    @Override
    public final synchronized Metadata getMetadata() throws DataStoreException {
        if (metadata == null) try {
            final MetadataBuilder builder = new MetadataBuilder();
            String format = reader().getFormatName();
            for (final String key : KNOWN_FORMATS) {
                if (key.equalsIgnoreCase(format)) {
                    try {
                        builder.setPredefinedFormat(key);
                        format = null;
                    } catch (MetadataStoreException e) {
                        listeners.warning(Level.FINE, null, e);
                    }
                    break;
                }
            }
            builder.addFormatName(format);                          // Does nothing if `format` is null.
            builder.addResourceScope(ScopeCode.COVERAGE, null);
            builder.addSpatialRepresentation(null, getGridGeometry(MAIN_IMAGE), true);
            if (gridGeometry.isDefined(GridGeometry.ENVELOPE)) {
                builder.addExtent(gridGeometry.getEnvelope());
            }
            addTitleOrIdentifier(builder);
            builder.setISOStandards(false);
            metadata = builder.buildAndFreeze();
        } catch (IOException e) {
            throw new DataStoreException(e);
        } catch (TransformException e) {
            throw new DataStoreReferencingException(e);
        }
        return metadata;
    }

    /**
     * Returns all images in this store. Note that fetching the size of the list is a potentially costly operation.
     *
     * @return list of images in this store.
     */
    @Override
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public final synchronized Collection<? extends GridCoverageResource> components() throws DataStoreException {
        if (components == null) try {
            components = new Components(reader().getNumImages(false));
        } catch (IOException e) {
            throw new DataStoreException(e);
        }
        return components;
    }

    /**
     * Returns all images in this store, or {@code null} if none and {@code create} is false.
     *
     * @param  create     whether to create the component list if it was not already created.
     * @param  numImages  number of images, or any negative value if unknown.
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    final Components components(final boolean create, final int numImages) {
        if (components == null && create) {
            components = new Components(numImages);
        }
        return components;
    }

    /**
     * A list of images where each {@link WorldFileResource} instance is initialized when first needed.
     * Fetching the list size may be a costly operation and will be done only if requested.
     */
    final class Components extends ListOfUnknownSize<WorldFileResource> {
        /**
         * Size of this list, or any negative value if unknown.
         */
        private int size;

        /**
         * All elements in this list. Some array elements may be {@code null} if the image
         * has never been requested.
         */
        private WorldFileResource[] images;

        /**
         * Creates a new list of images.
         *
         * @param  numImages  number of images, or any negative value if unknown.
         */
        private Components(final int numImages) {
            size = numImages;
            images = new WorldFileResource[Math.max(numImages, 1)];
        }

        /**
         * Returns the number of images in this list.
         * This method may be costly when invoked for the first time.
         */
        @Override
        public int size() {
            synchronized (WorldFileStore.this) {
                if (size < 0) try {
                    size   = reader().getNumImages(true);
                    images = ArraysExt.resize(images, size);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (DataStoreException e) {
                    throw new BackingStoreException(e);
                }
                return size;
            }
        }

        /**
         * Returns the number of images if this information is known, or any negative value otherwise.
         * This is used by {@link ListOfUnknownSize} for optimizing some operations.
         */
        @Override
        protected int sizeIfKnown() {
            synchronized (WorldFileStore.this) {
                return size;
            }
        }

        /**
         * Returns {@code true} if an element exists at the given index.
         * Current implementations is not more efficient than {@link #get(int)}.
         */
        @Override
        protected boolean exists(final int index) {
            synchronized (WorldFileStore.this) {
                if (size >= 0) {
                    return index >= 0 && index < size;
                }
                try {
                    return get(index) != null;
                } catch (IndexOutOfBoundsException e) {
                    return false;
                }
            }
        }

        /**
         * Returns the image at the given index. New instances are created when first requested.
         *
         * @param  index  index of the image for which to get a resource.
         * @return resource for the image identified by the given index.
         * @throws IndexOutOfBoundsException if the image index is out of bounds.
         */
        @Override
        public WorldFileResource get(final int index) {
            synchronized (WorldFileStore.this) {
                WorldFileResource image = null;
                if (index < images.length) {
                    image = images[index];
                }
                if (image == null) try {
                    image = createImageResource(index);
                    if (index >= images.length) {
                        images = Arrays.copyOf(images, Math.max(images.length * 2, index + 1));
                    }
                    images[index] = image;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (DataStoreException e) {
                    throw new BackingStoreException(e);
                }
                return image;
            }
        }

        /**
         * Invoked <em>after</em> an image has been added to the image file.
         * This method adds in this list a reference to the newly added file.
         *
         * @param  image  the image to add to this list.
         */
        final void added(final WorldFileResource image) {
            size = image.getImageIndex();
            if (size >= images.length) {
                images = Arrays.copyOf(images, size * 2);
            }
            images[size++] = image;
        }

        /**
         * Invoked <em>after</em> an image has been removed from the image file.
         * This method performs no bounds check (it must be done by the caller).
         *
         * @param  index  index of the image that has been removed.
         */
        final void removed(int index) throws DataStoreException {
            final int last = images.length - 1;
            System.arraycopy(images, index+1, images, index, last - index);
            images[last] = null;
            size--;
            while (index < last) {
                final WorldFileResource image = images[index++];
                if (image != null) image.decrementImageIndex();
            }
        }

        /**
         * Removes the element at the specified position in this list.
         */
        @Override
        public WorldFileResource remove(final int index) {
            final WorldFileResource image = get(index);
            try {
                WorldFileStore.this.remove(image);
            } catch (DataStoreException e) {
                throw new UnsupportedOperationException(e);
            }
            return image;
        }
    }

    /**
     * Invoked by {@link Components} when the caller want to remove a resource.
     * The actual implementation is provided by {@link WritableStore}.
     */
    void remove(final Resource resource) throws DataStoreException {
        throw new ReadOnlyStorageException();
    }

    /**
     * Creates a {@link GridCoverageResource} for the specified image.
     * This method is invoked by {@link Components} when first needed
     * and the result is cached by the caller.
     *
     * @param  index  index of the image for which to create a resource.
     * @return resource for the image identified by the given index.
     * @throws IndexOutOfBoundsException if the image index is out of bounds.
     */
    WorldFileResource createImageResource(final int index) throws DataStoreException, IOException {
        return new WorldFileResource(this, listeners, index, getGridGeometry(index));
    }

    /**
     * Prepares an image reader compatible with the writer and sets its input.
     * This method is invoked for switching from write mode to read mode.
     * Its actual implementation is provided by {@link WritableResource}.
     *
     * @param  current  the current image reader, or {@code null} if none.
     * @return the image reader to use, or {@code null} if none.
     * @throws IOException if an error occurred while preparing the reader.
     */
    ImageReader prepareReader(ImageReader current) throws IOException {
        return null;
    }

    /**
     * Returns the reader without doing any validation. The reader may be {@code null} either
     * because the store is closed or because the store is initially opened in write-only mode.
     * The reader may have a {@code null} input.
     */
    final ImageReader getCurrentReader() {
        return reader;
    }

    /**
     * Returns the reader if it has not been closed.
     *
     * @throws DataStoreClosedException if this data store is closed.
     * @throws IOException if an error occurred while preparing the reader.
     */
    final ImageReader reader() throws DataStoreException, IOException {
        assert Thread.holdsLock(this);
        ImageReader current = reader;
        if (current == null || current.getInput() == null) {
            reader = current = prepareReader(current);
            if (current == null) {
                throw new DataStoreClosedException(getLocale(), WorldFileStoreProvider.NAME, StandardOpenOption.READ);
            }
            configureReader();
        }
        return current;
    }

    /**
     * Closes this data store and releases any underlying resources.
     *
     * @throws DataStoreException if an error occurred while closing this data store.
     */
    @Override
    public synchronized void close() throws DataStoreException {
        final ImageReader codec = reader;
        reader       = null;
        metadata     = null;
        components   = null;
        gridGeometry = null;
        if (codec != null) try {
            final Object input = codec.getInput();
            codec.setInput(null);
            codec.dispose();
            if (input instanceof AutoCloseable) {
                ((AutoCloseable) input).close();
            }
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }
}
