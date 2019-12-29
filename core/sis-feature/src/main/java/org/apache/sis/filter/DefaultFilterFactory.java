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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.opengis.filter.*;
import org.opengis.filter.capability.*;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.identity.FeatureId;
import org.opengis.filter.identity.GmlObjectId;
import org.opengis.filter.identity.Identifier;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.*;
import org.opengis.geometry.Envelope;
import org.opengis.geometry.Geometry;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.FactoryException;
import org.opengis.util.GenericName;
import org.apache.sis.internal.system.Modules;
import org.apache.sis.internal.system.SystemListener;
import org.apache.sis.internal.feature.FunctionRegister;
import org.apache.sis.internal.feature.Resources;
import org.apache.sis.internal.filter.sqlmm.SQLMM;
import org.apache.sis.referencing.CRS;
import org.apache.sis.util.collection.BackingStoreException;

import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.geometry.ImmutableEnvelope;


/**
 * Default implementation of GeoAPI filter factory for creation of {@link Filter} and {@link Expression} instances.
 *
 * <div class="warning"><b>Warning:</b> most methods in this class are still unimplemented.
 * This is a very early draft subject to changes.
 * <b>TODO: the API of this class needs severe revision! DO NOT RELEASE.</b>
 * See <a href="https://github.com/opengeospatial/geoapi/issues/32">GeoAPI issue #32</a>.</div>
 *
 * @author  Johann Sorel (Geomatys)
 * @version 1.1
 * @since   1.1
 * @module
 */
public class DefaultFilterFactory implements FilterFactory2 {
    /**
     * All functions identified by a name like {@code "cos"}, {@code "hypot"}, <i>etc</i>.
     * The actual function creations is delegated to an external factory such as {@link SQLMM}.
     * The factories are fetched by {@link #function(String, Expression...)} when first needed.
     * This factory is cleared if classpath changes, for allowing dynamic reloading.
     *
     * @see #function(String, Expression...)
     */
    private static final Map<String,FunctionRegister> FUNCTION_REGISTERS = new HashMap<>();
    static {
        SystemListener.add(new SystemListener(Modules.FEATURE) {
            @Override protected void classpathChanged() {
                synchronized (FUNCTION_REGISTERS) {
                    FUNCTION_REGISTERS.clear();
                }
            }
        });
    }

    /**
     * According to OGC Filter encoding v2.0, comparison operators should default to cas sensitive comparison. We'll
     * use this constant to model it, so it will be easier to change default value is the standard evolves.
     * Doc reference : OGC 09-026r1 and ISO 19143:2010(E), section 7.7.3.2
     */
    private static final boolean DEFAULT_MATCH_CASE = true;

    /**
     * Creates a new factory.
     */
    public DefaultFilterFactory() {
    }

    // SPATIAL FILTERS /////////////////////////////////////////////////////////

    /**
     * Creates an operator that evaluates to {@code true} when the bounding box of the feature's geometry overlaps
     * the given bounding box.
     *
     * @param  propertyName  name of geometry property (for a {@link PropertyName} to access a feature's Geometry)
     * @param  minx          minimum "x" value (for a literal envelope).
     * @param  miny          minimum "y" value (for a literal envelope).
     * @param  maxx          maximum "x" value (for a literal envelope).
     * @param  maxy          maximum "y" value (for a literal envelope).
     * @param  srs           identifier of the Coordinate Reference System to use for a literal envelope.
     * @return operator that evaluates to {@code true} when the bounding box of the feature's geometry overlaps
     *         the bounding box provided in arguments to this method.
     *
     * @see #bbox(Expression, Envelope)
     */
    @Override
    public BBOX bbox(final String propertyName, final double minx,
            final double miny, final double maxx, final double maxy, final String srs)
    {
        return bbox(property(propertyName), minx, miny, maxx, maxy, srs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BBOX bbox(final Expression e, final double minx, final double miny,
            final double maxx, final double maxy, final String srs)
    {
        final CoordinateReferenceSystem crs = readCrs(srs);
        final GeneralEnvelope env = new GeneralEnvelope(2);
        env.setEnvelope(minx, miny, maxx, maxy);
        if (crs != null) env.setCoordinateReferenceSystem(crs);
        return bbox(e, new ImmutableEnvelope(env));
    }

    /**
     * Try to decode a full {@link CoordinateReferenceSystem} from given text. First, we try to interpret it as a code,
     * and if it fails, we try to read it as a WKT.
     *
     * @param srs The text describing the system. If null or blank, a null value is returned.
     * @return Possible null value if input text is empty.
     * @throws BackingStoreException If an error occurs while decoding the text.
     */
    private static CoordinateReferenceSystem readCrs(String srs) {
        if (srs == null || (srs = srs.trim()).isEmpty()) return null;
        try {
            return CRS.forCode(srs);
        } catch (NoSuchAuthorityCodeException e) {
            try {
                return CRS.fromWKT(srs);
            } catch (FactoryException bis) {
                e.addSuppressed(bis);
            }
            throw new BackingStoreException(e);
        } catch (FactoryException e) {
            throw new BackingStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BBOX bbox(final Expression e, final Envelope bounds) {
        return new DefaultBBOX(e, literal(bounds));
    }

    /**
     * Creates an operator that checks if all of a feature's geometry is more distant than the given distance
     * from the given geometry.
     *
     * @param  propertyName  name of geometry property (for a {@link PropertyName} to access a feature's Geometry).
     * @param  geometry      the geometry from which to evaluate the distance.
     * @param  distance      minimal distance for evaluating the expression as {@code true}.
     * @param  units         units of the given {@code distance}.
     * @return operator that evaluates to {@code true} when all of a feature's geometry is more distant than
     *         the given distance from the given geometry.
     */
    @Override
    public Beyond beyond(final String propertyName, final Geometry geometry,
            final double distance, final String units)
    {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return beyond(name, geom, distance, units);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Beyond beyond(final Expression left, final Expression right,
            final double distance, final String units)
    {
        return new SpatialFunction.Beyond(left, right, distance, units);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Contains contains(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return contains(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Contains contains(final Expression left, final Expression right) {
        return new SpatialFunction.Contains(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Crosses crosses(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return crosses(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Crosses crosses(final Expression left, final Expression right) {
        return new SpatialFunction.Crosses(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Disjoint disjoint(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return disjoint(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Disjoint disjoint(final Expression left, final Expression right) {
        return new SpatialFunction.Disjoint(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DWithin dwithin(final String propertyName, final Geometry geometry,
            final double distance, final String units)
    {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return dwithin(name, geom, distance, units);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DWithin dwithin(final Expression left, final Expression right,
            final double distance, final String units)
    {
        return new SpatialFunction.DWithin(left, right, distance, units);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Equals equals(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return equal(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Equals equal(final Expression left, final Expression right) {
        return new SpatialFunction.Equals(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Intersects intersects(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return intersects(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Intersects intersects(final Expression left, final Expression right) {
        return new SpatialFunction.Intersects(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Overlaps overlaps(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return overlaps(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Overlaps overlaps(final Expression left, final Expression right) {
        return new SpatialFunction.Overlaps(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Touches touches(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return touches(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Touches touches(final Expression left, final Expression right) {
        return new SpatialFunction.Touches(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Within within(final String propertyName, final Geometry geometry) {
        final PropertyName name = property(propertyName);
        final Literal geom = literal(geometry);
        return within(name, geom);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Within within(final Expression left, final Expression right) {
        return new SpatialFunction.Within(left, right);
    }

    // IDENTIFIERS /////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public FeatureId featureId(final String id) {
        return new DefaultObjectId(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GmlObjectId gmlObjectId(final String id) {
        return new DefaultObjectId(id);
    }

    // FILTERS /////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public And and(final Filter filter1, final Filter filter2) {
        return and(Arrays.asList(filter1, filter2));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public And and(final List<Filter> filters) {
        return new LogicalFunction.And(filters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Or or(final Filter filter1, final Filter filter2) {
        return or(Arrays.asList(filter1, filter2));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Or or(final List<Filter> filters) {
        return new LogicalFunction.Or(filters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Not not(final Filter filter) {
        return new UnaryFunction.Not(filter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Id id(final Set<? extends Identifier> ids) {
        return new FilterByIdentifier(ids);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyName property(final GenericName name) {
        return property(name.toString());
    }

    /**
     * Creates a new expression retrieving values from a property of the given name.
     *
     * @param  name  name of the property (usually a feature attribute).
     */
    @Override
    public PropertyName property(final String name) {
        return new LeafExpression.Property(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsBetween between(final Expression expression, final Expression lower, final Expression upper) {
        return new ComparisonFunction.Between(expression, lower, upper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsEqualTo equals(final Expression expression1, final Expression expression2) {
        return equal(expression1, expression2, DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsEqualTo equal(final Expression expression1, final Expression expression2,
                                   final boolean isMatchingCase, final MatchAction matchAction)
    {
        return new ComparisonFunction.EqualTo(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsNotEqualTo notEqual(final Expression expression1, final Expression expression2) {
        return notEqual(expression1, expression2, DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsNotEqualTo notEqual(final Expression expression1, final Expression expression2,
                                         final boolean isMatchingCase, final MatchAction matchAction)
    {
        return new ComparisonFunction.NotEqualTo(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsGreaterThan greater(final Expression expression1, final Expression expression2) {
        return greater(expression1,expression2,DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsGreaterThan greater(final Expression expression1, final Expression expression2,
                                         final boolean isMatchingCase, final MatchAction matchAction)
    {
        return new ComparisonFunction.GreaterThan(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsGreaterThanOrEqualTo greaterOrEqual(final Expression expression1, final Expression expression2) {
        return greaterOrEqual(expression1, expression2,DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsGreaterThanOrEqualTo greaterOrEqual(final Expression expression1, final Expression expression2,
                                                         final boolean isMatchingCase, final MatchAction matchAction)
    {
        return new ComparisonFunction.GreaterThanOrEqualTo(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLessThan less(final Expression expression1, final Expression expression2) {
        return less(expression1, expression2, DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLessThan less(final Expression expression1, final Expression expression2,
                                   final boolean isMatchingCase, MatchAction matchAction)
    {
        return new ComparisonFunction.LessThan(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLessThanOrEqualTo lessOrEqual(final Expression expression1, final Expression expression2) {
        return lessOrEqual(expression1, expression2, DEFAULT_MATCH_CASE, MatchAction.ANY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLessThanOrEqualTo lessOrEqual(final Expression expression1, final Expression expression2,
                                                   final boolean isMatchingCase, final MatchAction matchAction)
    {
        return new ComparisonFunction.LessThanOrEqualTo(expression1, expression2, isMatchingCase, matchAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLike like(final Expression expression, final String pattern) {
        return like(expression, pattern, "*", "?", "\\");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLike like(final Expression expression, final String pattern,
            final String wildcard, final String singleChar, final String escape)
    {
        return like(expression,pattern,wildcard,singleChar,escape,DEFAULT_MATCH_CASE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsLike like(final Expression expression, final String pattern,
            final String wildcard, final String singleChar,
            final String escape, final boolean isMatchingCase)
    {
        return new DefaultLike(expression, pattern, wildcard, singleChar, escape, isMatchingCase);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsNull isNull(final Expression expression) {
        return new UnaryFunction.IsNull(expression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PropertyIsNil isNil(Expression expression) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // TEMPORAL FILTER /////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public After after(Expression expression1, Expression expression2) {
        return new TemporalFunction.After(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AnyInteracts anyInteracts(Expression expression1, Expression expression2) {
        return new TemporalFunction.AnyInteracts(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Before before(Expression expression1, Expression expression2) {
        return new TemporalFunction.Before(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Begins begins(Expression expression1, Expression expression2) {
        return new TemporalFunction.Begins(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BegunBy begunBy(Expression expression1, Expression expression2) {
        return new TemporalFunction.BegunBy(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public During during(Expression expression1, Expression expression2) {
        return new TemporalFunction.During(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Ends ends(Expression expression1, Expression expression2) {
        return new TemporalFunction.Ends(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EndedBy endedBy(Expression expression1, Expression expression2) {
        return new TemporalFunction.EndedBy(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Meets meets(Expression expression1, Expression expression2) {
        return new TemporalFunction.Meets(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetBy metBy(Expression expression1, Expression expression2) {
        return new TemporalFunction.MetBy(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OverlappedBy overlappedBy(Expression expression1, Expression expression2) {
        return new TemporalFunction.OverlappedBy(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TContains tcontains(Expression expression1, Expression expression2) {
        return new TemporalFunction.Contains(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TEquals tequals(Expression expression1, Expression expression2) {
        return new TemporalFunction.Equals(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TOverlaps toverlaps(Expression expression1, Expression expression2) {
        return new TemporalFunction.Overlaps(expression1, expression2);
    }

    // EXPRESSIONS /////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public Add add(final Expression expression1, final Expression expression2) {
        return new ArithmeticFunction.Add(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Divide divide(final Expression expression1, final Expression expression2) {
        return new ArithmeticFunction.Divide(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Multiply multiply(final Expression expression1, final Expression expression2) {
        return new ArithmeticFunction.Multiply(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subtract subtract(final Expression expression1, final Expression expression2) {
        return new ArithmeticFunction.Subtract(expression1, expression2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Function function(final String name, final Expression... parameters) {
        final FunctionRegister register;
        synchronized (FUNCTION_REGISTERS) {
            if (FUNCTION_REGISTERS.isEmpty()) {
                /*
                 * Load functions when first needed or if classpath changed since last invocation.
                 * The SQLMM factory is hard-coded because it is considered as a basic service to
                 * be provided by all DefaultFilterFactory implementations, and for avoiding the
                 * need to make SQLMM class public.
                 */
                final SQLMM r = new SQLMM();
                for (final String fn : r.getNames()) {
                    FUNCTION_REGISTERS.put(fn, r);
                }
                for (final FunctionRegister er : ServiceLoader.load(FunctionRegister.class)) {
                    for (final String fn : er.getNames()) {
                        FUNCTION_REGISTERS.putIfAbsent(fn, er);
                    }
                }
            }
            register = FUNCTION_REGISTERS.get(name);
        }
        if (register == null) {
            throw new IllegalArgumentException(Resources.format(Resources.Keys.UnknownFunction_1, name));
        }
        return register.create(name, parameters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final Object value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final byte value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final short value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final int value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final long value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final float value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final double value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final char value) {
        return new LeafExpression.Literal(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Literal literal(final boolean value) {
        return new LeafExpression.Literal(value);
    }

    // SORT BY /////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public SortBy sort(final String propertyName, final SortOrder order) {
        return new DefaultSortBy(property(propertyName), order);
    }

    // CAPABILITIES ////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    @Override
    public Operator operator(final String name) {
        return new Capabilities.Operator(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpatialOperator spatialOperator(final String name, final GeometryOperand[] geometryOperands) {
        return new Capabilities.SpatialOperator(name, geometryOperands);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionName functionName(final String name, final int nargs) {
        return new Capabilities.FunctionName(name, null, nargs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Functions functions(final FunctionName[] functionNames) {
        return new Capabilities.Functions(functionNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpatialOperators spatialOperators(final SpatialOperator[] spatialOperators) {
        return new Capabilities.SpatialOperators(spatialOperators);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComparisonOperators comparisonOperators(final Operator[] comparisonOperators) {
        return new Capabilities.ComparisonOperators(comparisonOperators);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ArithmeticOperators arithmeticOperators(final boolean simple, final Functions functions) {
        return new Capabilities.ArithmeticOperators(simple, functions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScalarCapabilities scalarCapabilities(final ComparisonOperators comparison,
            final ArithmeticOperators arithmetic, final boolean logical)
    {
        return new Capabilities.ScalarCapabilities(logical, comparison, arithmetic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SpatialCapabilities spatialCapabilities(
            final GeometryOperand[] geometryOperands, final SpatialOperators spatial)
    {
        return new Capabilities.SpatialCapabilities(geometryOperands, spatial);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdCapabilities idCapabilities(final boolean eid, final boolean fid) {
        return new Capabilities.IdCapabilities(eid, fid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FilterCapabilities capabilities(final String version,
            final ScalarCapabilities scalar, final SpatialCapabilities spatial,
            final TemporalCapabilities temporal, final IdCapabilities id)
    {
        return new Capabilities.FilterCapabilities(version, id, spatial, scalar, temporal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TemporalCapabilities temporalCapabilities(TemporalOperand[] temporalOperands, TemporalOperators temporal) {
        return new Capabilities.TemporalCapabilities(temporalOperands, temporal);
    }
}