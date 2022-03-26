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
package org.apache.sis.internal.storage;

import java.util.Locale;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;
import java.util.concurrent.TimeUnit;
import java.math.RoundingMode;
import java.awt.image.RasterFormatException;
import org.opengis.geometry.Envelope;
import org.opengis.metadata.Metadata;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreReferencingException;
import org.apache.sis.storage.NoSuchDataException;
import org.apache.sis.storage.GridCoverageResource;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.coverage.grid.GridGeometry;
import org.apache.sis.coverage.grid.GridExtent;
import org.apache.sis.coverage.grid.DisjointExtentException;
import org.apache.sis.measure.Latitude;
import org.apache.sis.measure.Longitude;
import org.apache.sis.measure.AngleFormat;
import org.apache.sis.util.logging.PerformanceLevel;
import org.apache.sis.internal.storage.io.IOUtilities;
import org.apache.sis.internal.util.StandardDateFormat;
import org.apache.sis.internal.jdk9.JDK9;


/**
 * Base class for implementations of {@link GridCoverageResource}.
 * This class provides default implementations of {@code Resource} methods.
 * Those default implementations get data from the following abstract methods:
 *
 * <ul>
 *   <li>{@link #getGridGeometry()}</li>
 *   <li>{@link #getSampleDimensions()}</li>
 * </ul>
 *
 * This class also provides the following helper methods for implementation
 * of the {@link #read(GridGeometry, int...) read(…)} method in subclasses:
 *
 * <ul>
 *   <li>{@link #canNotRead(String, GridGeometry, Throwable)} for reporting a failure to read operation.</li>
 *   <li>{@link #logReadOperation(Object, GridGeometry, long)} for logging a notice about a read operation.</li>
 * </ul>
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.2
 * @since   0.8
 * @module
 */
public abstract class AbstractGridResource extends AbstractResource implements GridCoverageResource {
    /**
     * Creates a new resource.
     *
     * @param  parent  listeners of the parent resource, or {@code null} if none.
     *         This is usually the listeners of the {@link DataStore} that created this resource.
     */
    protected AbstractGridResource(final StoreListeners parent) {
        super(parent);
    }

    /**
     * Returns the grid geometry envelope if known.
     * This implementation fetches the envelope from the grid geometry.
     * The envelope is absent if the grid geometry does not provide this information.
     *
     * @return the grid geometry envelope.
     * @throws DataStoreException if an error occurred while computing the grid geometry.
     */
    @Override
    public Optional<Envelope> getEnvelope() throws DataStoreException {
        final GridGeometry gg = getGridGeometry();
        if (gg != null && gg.isDefined(GridGeometry.ENVELOPE)) {
            return Optional.of(gg.getEnvelope());
        }
        return Optional.empty();
    }

    /**
     * Invoked in a synchronized block the first time that {@code getMetadata()} is invoked.
     * The default implementation populates metadata based on information provided by
     * {@link #getIdentifier()       getIdentifier()},
     * {@link #getEnvelope()         getEnvelope()},
     * {@link #getGridGeometry()     getGridGeometry()} and
     * {@link #getSampleDimensions() getSampleDimensions()}.
     * Subclasses should override if they can provide more information.
     *
     * @return the newly created metadata, or {@code null} if unknown.
     * @throws DataStoreException if an error occurred while reading metadata from this resource.
     */
    @Override
    protected Metadata createMetadata() throws DataStoreException {
        final MetadataBuilder builder = new MetadataBuilder();
        builder.addDefaultMetadata(this, listeners);
        return builder.build(true);
    }

    /**
     * Creates an exception for a failure to load data. If the failure may be caused by an envelope
     * outside the resource domain, that envelope will be inferred from the {@code request} argument.
     *
     * @param  filename  some identification (typically a file name) of the data that can not be read.
     * @param  request   the requested domain, or {@code null} if unspecified.
     * @param  cause     the cause of the failure, or {@code null} if none.
     * @return the exception to throw.
     */
    protected final DataStoreException canNotRead(final String filename, final GridGeometry request, Throwable cause) {
        final int DOMAIN = 1, REFERENCING = 2, CONTENT = 3;
        int type = 0;               // One of above constants, with 0 for "none of above".
        Envelope bounds = null;
        if (cause instanceof DisjointExtentException) {
            type = DOMAIN;
            if (request != null && request.isDefined(GridGeometry.ENVELOPE)) {
                bounds = request.getEnvelope();
            }
        } else if (cause instanceof RuntimeException) {
            Throwable c = cause.getCause();
            if (isReferencing(c)) {
                type = REFERENCING;
                cause = c;
            } else if (cause instanceof ArithmeticException || cause instanceof RasterFormatException) {
                type = CONTENT;
            }
        } else if (isReferencing(cause)) {
            type = REFERENCING;
        }
        final String message = createExceptionMessage(filename, bounds);
        switch (type) {
            case DOMAIN:      return new NoSuchDataException(message, cause);
            case REFERENCING: return new DataStoreReferencingException(message, cause);
            case CONTENT:     return new DataStoreContentException(message, cause);
            default:          return new DataStoreException(message, cause);
        }
    }

    /**
     * Returns {@code true} if the given exception is {@link FactoryException} or {@link TransformException}.
     * This is for deciding if an exception should be rethrown as an {@link DataStoreReferencingException}.
     *
     * @param  cause  the exception to verify.
     * @return whether the given exception is {@link FactoryException} or {@link TransformException}.
     */
    private static boolean isReferencing(final Throwable cause) {
        return (cause instanceof FactoryException || cause instanceof TransformException);
    }

    /**
     * Logs the execution of a {@link #read(GridGeometry, int...)} operation.
     * The log level will be {@link Level#FINE} if the operation was quick enough,
     * or {@link PerformanceLevel#SLOW} or higher level otherwise.
     *
     * @param  file       the file that was opened, or {@code null} for {@link #getSourceName()}.
     * @param  domain     domain of the created grid coverage.
     * @param  startTime  value of {@link System#nanoTime()} when the loading process started.
     */
    protected final void logReadOperation(final Object file, final GridGeometry domain, final long startTime) {
        final Logger logger = listeners.getLogger();
        final long   nanos  = System.nanoTime() - startTime;
        final Level  level  = PerformanceLevel.forDuration(nanos, TimeUnit.NANOSECONDS);
        if (logger.isLoggable(level)) {
            final Locale locale = listeners.getLocale();
            final Object[] parameters = new Object[6];
            parameters[0] = IOUtilities.filename(file != null ? file : listeners.getSourceName());
            parameters[5] = nanos / (double) StandardDateFormat.NANOS_PER_SECOND;
            JDK9.ifPresentOrElse(domain.getGeographicExtent(), (box) -> {
                final AngleFormat f = new AngleFormat(locale);
                double min = box.getSouthBoundLatitude();
                double max = box.getNorthBoundLatitude();
                f.setPrecision(max - min, true);
                f.setRoundingMode(RoundingMode.FLOOR);   parameters[1] = f.format(new Latitude(min));
                f.setRoundingMode(RoundingMode.CEILING); parameters[2] = f.format(new Latitude(max));
                min = box.getWestBoundLongitude();
                max = box.getEastBoundLongitude();
                f.setPrecision(max - min, true);
                f.setRoundingMode(RoundingMode.FLOOR);   parameters[3] = f.format(new Longitude(min));
                f.setRoundingMode(RoundingMode.CEILING); parameters[4] = f.format(new Longitude(max));
            }, () -> {
                // If no geographic coordinates, fallback on the 2 first dimensions.
                if (domain.isDefined(GridGeometry.ENVELOPE)) {
                    final Envelope box = domain.getEnvelope();
                    final int dimension = Math.min(box.getDimension(), 2);
                    for (int t=1, i=0; i<dimension; i++) {
                        parameters[t++] = box.getMinimum(i);
                        parameters[t++] = box.getMaximum(i);
                    }
                } else if (domain.isDefined(GridGeometry.EXTENT)) {
                    final GridExtent box = domain.getExtent();
                    final int dimension = Math.min(box.getDimension(), 2);
                    for (int t=1, i=0; i<dimension; i++) {
                        parameters[t++] = box.getLow (i);
                        parameters[t++] = box.getHigh(i);
                    }
                }
            });
            final LogRecord record = Resources.forLocale(locale)
                    .getLogRecord(level, Resources.Keys.LoadedGridCoverage_6, parameters);
            record.setSourceClassName(GridCoverageResource.class.getName());
            record.setSourceMethodName("read");
            record.setLoggerName(logger.getName());
            logger.log(record);
        }
    }
}
