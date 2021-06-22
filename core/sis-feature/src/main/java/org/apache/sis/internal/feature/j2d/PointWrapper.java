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
package org.apache.sis.internal.feature.j2d;

import java.awt.Shape;
import java.awt.geom.Point2D;
import java.util.Iterator;
import org.opengis.geometry.DirectPosition;
import org.apache.sis.geometry.DirectPosition2D;
import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.internal.feature.Geometries;
import org.apache.sis.internal.feature.GeometryWithCRS;
import org.apache.sis.internal.feature.GeometryWrapper;
import org.apache.sis.internal.filter.sqlmm.SQLMM;
import org.apache.sis.util.Debug;


/**
 * The wrapper of Java2D points. Has to be provided in a separated class because
 * {@link Point2D} are not {@link Shape} in Java2D API.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
final class PointWrapper extends GeometryWithCRS<Shape> {
    /**
     * The wrapped implementation.
     */
    private final Point2D point;

    /**
     * Creates a new wrapper around the given point.
     */
    PointWrapper(final Point2D point) {
        this.point = point;
    }

    /**
     * Returns the implementation-dependent factory of geometric object.
     */
    @Override
    public Geometries<Shape> factory() {
        return Factory.INSTANCE;
    }

    /**
     * Returns the point specified at construction time.
     */
    @Override
    public Object implementation() {
        return point;
    }

    /**
     * Returns an empty envelope centered on this point.
     */
    @Override
    public GeneralEnvelope getEnvelope() {
        final GeneralEnvelope env = createEnvelope();
        final double x = point.getX();
        final double y = point.getY();
        env.setRange(0, x, y);
        env.setRange(1, x, y);
        return env;
    }

    /**
     * Returns the centroid of the wrapped geometry as a direct position.
     */
    @Override
    public DirectPosition getCentroid() {
        return new DirectPosition2D(getCoordinateReferenceSystem(), point.getX(), point.getY());
    }

    /**
     * Returns the point coordinates.
     */
    @Override
    public double[] getPointCoordinates() {
        return new double[] {
            point.getX(),
            point.getY()
        };
    }

    /**
     * Returns all coordinate tuples in the wrapped geometry.
     * This method is currently used for testing purpose only.
     */
    @Debug
    @Override
    public double[] getAllCoordinates() {
        return getPointCoordinates();
    }

    /**
     * Merges a sequence of points or paths after this geometry.
     *
     * @throws ClassCastException if an element in the iterator is not a {@link Shape} or a {@link Point2D}.
     */
    @Override
    protected Shape mergePolylines(final Iterator<?> polylines) {
        return Wrapper.mergePolylines(point, polylines);
    }

    /**
     * Applies a SQLMM operation on this geometry.
     *
     * @param  operation  the SQLMM operation to apply.
     * @param  other      the other geometry, or {@code null} if the operation requires only one geometry.
     * @param  argument   an operation-specific argument, or {@code null} if not applicable.
     * @return result of the specified operation.
     */
    @Override
    protected Object operationSameCRS(final SQLMM operation, final GeometryWrapper<Shape> other, final Object argument) {
        switch (operation) {
            case ST_Centroid: return point.clone();
            default: return super.operationSameCRS(operation, other, argument);
        }
    }

    /**
     * Builds a WKT representation of the wrapped point.
     */
    @Override
    public String formatWKT(final double flatness) {
        return getCentroid().toString();
    }
}
