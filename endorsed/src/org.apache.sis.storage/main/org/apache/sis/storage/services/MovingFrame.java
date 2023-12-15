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
package org.apache.sis.storage.services;

import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;
import org.opengis.util.FactoryException;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.Transformation;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransformFactory;
import org.opengis.referencing.operation.NoninvertibleTransformException;
import org.opengis.geometry.Envelope;
import org.opengis.feature.Feature;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.parameter.Parameters;
import org.apache.sis.parameter.ParameterBuilder;
import org.apache.sis.referencing.ImmutableIdentifier;
import org.apache.sis.referencing.factory.FactoryDataException;
import org.apache.sis.referencing.operation.DefaultOperationMethod;
import org.apache.sis.referencing.operation.transform.MathTransformProvider;
import org.apache.sis.referencing.operation.gridded.GridFile;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.DataStores;
import org.apache.sis.storage.FeatureSet;
import org.apache.sis.util.internal.Constants;
import org.apache.sis.util.collection.BackingStoreException;


/**
 * Provider of the <q>Trajectory file</q> coordinate operation. This operation is a transformation performing
 * a datum shift from one CRS to another with a time-varying translation, with the assumption that both CRS
 * have axes oriented in the same directions. The target CRS is attached to a feature (e.g., a spacecraft)
 * moving relatively to the source CRS. The transformation is defined by a Moving Feature file providing
 * the trajectory of the moving object. For each (<var>x</var>,<var>y</var>,<var>z</var>,<var>t</var>)
 * source coordinate tuples, the operation interpolates the position in the trajectory at time <var>t</var>.
 * The interpolated vector is subtracted from the source (<var>x</var>,<var>y</var>,<var>z</var>) coordinates
 * and the <var>t</var> coordinate is passed unchanged.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
public final class MovingFrame extends DefaultOperationMethod implements MathTransformProvider {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = 1945555009593827295L;

    /**
     * The operation parameter descriptor for the <q>Trajectory file</q> parameter value.
     */
    static final ParameterDescriptor<URI> FILE;

    /**
     * The group of all parameters expected by this coordinate operation.
     */
    static final ParameterDescriptorGroup PARAMETERS;
    static {
        final var identifier = new ImmutableIdentifier(Citations.OGC, Constants.OGC, "200", "1", null);
        final var builder = new ParameterBuilder().setCodeSpace(Citations.OGC, Constants.OGC);

        FILE       = builder.addIdentifier(identifier).addName("Trajectory file").create(URI.class, null);
        PARAMETERS = builder.addIdentifier(identifier).addName("Translation by trajectory").createGroup(FILE);
    }

    /**
     * Creates a new provider.
     */
    public MovingFrame() {
        super(Map.of(IDENTIFIERS_KEY, PARAMETERS.getIdentifiers().iterator().next(),
                     NAME_KEY,        PARAMETERS.getName()),
                     PARAMETERS);
    }

    /**
     * {@return the interface implemented by all coordinate operations that use this method}.
     */
    @Override
    public Class<Transformation> getOperationType() {
        return Transformation.class;
    }

    /**
     * Creates a transform from the specified group of parameter values.
     *
     * @param  factory  the factory to use if this constructor needs to create other math transforms.
     * @param  values   the group of parameter values.
     * @return the created math transform.
     * @throws FactoryException if an error occurred while loading the grid.
     */
    @Override
    public MathTransform createMathTransform(final MathTransformFactory factory, final ParameterValueGroup values)
            throws FactoryException
    {
        final GridFile file = new GridFile(Parameters.castOrWrap(values), FILE, null);
        /*
         * The file should be a set of features containing only one feature.
         * If there is more than one, the current implementation ignores the
         * extra ones and take only the first one.
         */
        Feature feature = null;
        Exception cause = null;
        CoordinateReferenceSystem crs = null;
        Object source = file.path().orElse(null);
        if (source == null) source = file.resolved();       // Cannot use `orElse(…)` because of different types.
        try (DataStore ds = DataStores.open(source)) {
            if (ds instanceof FeatureSet) {
                final var fs = (FeatureSet) ds;
                crs = fs.getEnvelope().map(Envelope::getCoordinateReferenceSystem)
                        .orElseThrow(TranslationByTrajectory::missingCRS);
                try (Stream<Feature> features = fs.features(false)) {
                    feature = features.findFirst().orElse(null);
                } catch (BackingStoreException e) {
                    throw e.unwrapOrRethrow(DataStoreException.class);
                }
            }
        } catch (DataStoreException e) {
            throw new FactoryDataException(e);
        }
        if (feature != null) try {
            return new TranslationByTrajectory(file.parameter, crs, feature).createCompleteTransform();
        } catch (NoninvertibleTransformException e) {
            throw new FactoryDataException(e);
        }
        throw file.canNotLoad(MovingFrame.class, "trajectory", cause);
    }
}
