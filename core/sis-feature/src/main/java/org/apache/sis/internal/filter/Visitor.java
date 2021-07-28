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
package org.apache.sis.internal.filter;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.function.BiConsumer;
import org.opengis.util.CodeList;
import org.apache.sis.internal.feature.Resources;

// Branch-dependent imports
import org.opengis.filter.Filter;
import org.opengis.filter.Expression;
import org.opengis.filter.LogicalOperatorName;
import org.opengis.filter.SpatialOperatorName;
import org.opengis.filter.DistanceOperatorName;
import org.opengis.filter.TemporalOperatorName;
import org.opengis.filter.ComparisonOperatorName;


/**
 * An executor of different actions depending on filter or expression type.
 * Action are defined by {@link BiConsumer} where the first parameter is the filter or expression,
 * and the second parameter is an arbitrary object used as accumulator. For example the accumulator
 * may be a {@link StringBuilder} where the filter is written as a SQL or CQL statement.
 *
 * <div class="note"><b>Relationship with the visitor pattern</b><br>
 * This class provides similar functionalities than the "visitor pattern".
 * The actions are defined by lambda functions in a {@link HashMap} instead than by overriding methods,
 * but the results are similar.</div>
 *
 * <h2>Thread-safety</h2>
 * {@code Visitor} instances are thread-safe if protected methods are invoked at construction time only.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.1
 *
 * @param  <R>  the type of resources (e.g. {@link org.opengis.feature.Feature}) used as inputs.
 * @param  <A>  type of the accumulator object where actions will write their results.
 *
 * @since 1.1
 * @module
 */
public abstract class Visitor<R,A> {
    /**
     * All filters known to this visitor.
     * May contain an entry associated to the {@code null} key.
     *
     * @see #setFilterHandler(CodeList, BiConsumer)
     */
    private final Map<CodeList<?>, BiConsumer<Filter<R>,A>> filters;

    /**
     * All expressions known to this visitor.
     * May contain an entry associated to the {@code null} key.
     *
     * @see #setExpressionHandler(String, BiConsumer)
     */
    private final Map<String, BiConsumer<Expression<R,?>,A>> expressions;

    /**
     * Creates a new visitor.
     */
    protected Visitor() {
        filters     = new HashMap<>();
        expressions = new HashMap<>();
    }

    /**
     * Creates a new visitor which will accept only the specified type of objects.
     *
     * @param  hasFilters      whether this filter will accepts filters.
     * @param  hasExpressions  whether this filter will accepts expressions.
     */
    protected Visitor(final boolean hasFilters, final boolean hasExpressions) {
        filters     = hasFilters     ? new HashMap<>() : Collections.emptyMap();
        expressions = hasExpressions ? new HashMap<>() : Collections.emptyMap();
    }

    /**
     * Returns the action to execute for the given type of filter.
     * The {@code null} type is legal and identifies the action to execute
     * when the {@link Filter} instance is null or has a null type.
     *
     * @param  type  identification of the filter type (can be {@code null}).
     * @return the action to execute when the identified filter is found, or {@code null} if none.
     */
    protected final BiConsumer<Filter<R>,A> getFilterHandler(final CodeList<?> type) {
        return filters.get(type);
    }

    /**
     * Returns the action to execute for the given type of expression.
     * The {@code null} type is legal and identifies the action to execute
     * when the {@link Expression} instance is null.
     *
     * @param  type  identification of the expression type (can be {@code null}).
     * @return the action to execute when the identified expression is found, or {@code null} if none.
     */
    protected final BiConsumer<Expression<R,?>,A> getExpressionHandler(final String type) {
        return expressions.get(type);
    }

    /**
     * Sets the action to execute for the given type of filter.
     * The {@code null} type is legal and identifies the action to execute
     * when the {@link Filter} instance is null or has a null type.
     *
     * @param  type    identification of the filter type (can be {@code null}).
     * @param  action  the action to execute when the identified filter is found.
     */
    protected final void setFilterHandler(final CodeList<?> type, final BiConsumer<Filter<R>,A> action) {
        filters.put(type, action);
    }

    /**
     * Sets the same action for all member of the same family of filters.
     * The action is set in enumeration declaration order up to the last type inclusive.
     *
     * @param  lastType  identification of the last filter type (inclusive).
     * @param  action    the action to execute when an identified filter is found.
     */
    private void setFamilyHandlers(final CodeList<?> lastType, final BiConsumer<Filter<R>,A> action) {
        for (final CodeList<?> type : lastType.family()) {
            filters.put(type, action);
            if (type == lastType) break;
        }
    }

    /**
     * Sets the action to execute for the given type of expression.
     * The {@code null} type is legal and identifies the action to execute
     * when the {@link Expression} instance is null.
     *
     * @param  type    identification of the expression type (can be {@code null}).
     * @param  action  the action to execute when the identified expression is found.
     */
    protected final void setExpressionHandler(final String type, final BiConsumer<Expression<R,?>,A> action) {
        expressions.put(type, action);
    }

    /**
     * Sets the same action to execute for the given types of expression.
     *
     * @param  types   identification of the expression types.
     * @param  action  the action to execute when the identified expression is found.
     */
    private void setExpressionHandlers(final BiConsumer<Expression<R,?>,A> action, final String... types) {
        for (final String type : types) {
            expressions.put(type, action);
        }
    }

    /**
     * Sets the same action to execute for the {@code AND},  {@code OR} and {@code NOT} types filter.
     *
     * @param  action  the action to execute when one of the enumerated filters is found.
     */
    protected final void setLogicalHandlers(final BiConsumer<Filter<R>,A> action) {
        setFamilyHandlers(LogicalOperatorName.NOT, action);
    }

    /**
     * Sets the same action for both the {@code IsNull} and {@code IsNil} types of filter.
     *
     * @param  action  the action to execute when one of the enumerated filters is found.
     */
    protected final void setNullAndNilHandlers(final BiConsumer<Filter<R>,A> action) {
        setFilterHandler(ComparisonOperatorName.valueOf(FunctionNames.PROPERTY_IS_NULL), action);
        setFilterHandler(ComparisonOperatorName.valueOf(FunctionNames.PROPERTY_IS_NIL),  action);
    }

    /**
     * Sets the same action to execute for the &lt;, &gt;, ≤, ≥, = and ≠ types of filter.
     *
     * @param  action  the action to execute when one of the enumerated filters is found.
     */
    protected final void setBinaryComparisonHandlers(final BiConsumer<Filter<R>,A> action) {
        setFamilyHandlers(ComparisonOperatorName.PROPERTY_IS_GREATER_THAN_OR_EQUAL_TO, action);
    }

    /**
     * Sets the same action to execute for the temporal comparison operators.
     * The operators are {@code AFTER}, {@code BEFORE}, {@code BEGINS}, {@code BEGUN_BY}, {@code CONTAINS},
     * {@code DURING}, {@code EQUALS}, {@code OVERLAPS}, {@code MEETS}, {@code ENDS}, {@code OVERLAPPED_BY},
     * {@code MET_BY}, {@code ENDED_BY} and {@code ANY_INTERACTS} types filter.
     *
     * @param  action  the action to execute when one of the enumerated filters is found.
     */
    protected final void setBinaryTemporalHandlers(final BiConsumer<Filter<R>,A> action) {
        setFamilyHandlers(TemporalOperatorName.ANY_INTERACTS, action);
    }

    /**
     * Sets the same action to execute for the spatial comparison operators, including the ones
     * with a distance parameter. The operators are {@code BBOX}, {@code EQUALS}, {@code DISJOINT},
     * {@code INTERSECTS}, {@code TOUCHES}, {@code CROSSES}, {@code WITHIN}, {@code CONTAINS},
     * {@code OVERLAPS}, {@code DWITHIN} and {@code BEYOND} types filter.
     *
     * @param  action  the action to execute when one of the enumerated filters is found.
     */
    protected final void setSpatialHandlers(final BiConsumer<Filter<R>,A> action) {
        setFamilyHandlers(SpatialOperatorName.OVERLAPS, action);
        setFamilyHandlers(DistanceOperatorName.WITHIN,  action);
    }

    /**
     * Sets the same action to execute for the +, −, × and ÷ expressions.
     *
     * @param  action  the action to execute when one of the enumerated expressions is found.
     */
    protected final void setMathHandlers(final BiConsumer<Expression<R,?>,A> action) {
        setExpressionHandlers(action, FunctionNames.Add, FunctionNames.Subtract, FunctionNames.Multiply, FunctionNames.Divide);
    }

    /**
     * Executes the registered action for the given filter.
     * Actions are registered by calls to {@code setFooHandler(…)} before the call to this {@code visit(…)} method.
     *
     * @param  filter       the filter for which to execute an action based on its type.
     * @param  accumulator  where to write the result of all actions.
     * @throws UnsupportedOperationException if there is no action registered for the given filter.
     */
    public void visit(final Filter<R> filter, final A accumulator) {
        final CodeList<?> type = (filter != null) ? filter.getOperatorType() : null;
        final BiConsumer<Filter<R>, A> f = filters.get(type);
        if (f != null) {
            f.accept(filter, accumulator);
        } else {
            typeNotFound(type, filter, accumulator);
        }
    }

    /**
     * Executes the registered action for the given expression.
     * Actions are registered by calls to {@code setFooHandler(…)} before the call to this {@code visit(…)} method.
     *
     * @param  expression   the expression for which to execute an action based on its type.
     * @param  accumulator  where to write the result of all actions.
     * @throws UnsupportedOperationException if there is no action registered for the given expression.
     */
    public final void visit(final Expression<R,?> expression, final A accumulator) {
        final String type = (expression != null) ? expression.getFunctionName().tip().toString() : null;
        final BiConsumer<Expression<R,?>, A> f = expressions.get(type);
        /*
         * This method signature uses `<? super R>` for caller's convenience because this is the type that
         * we get from `Expression.getParameters()` and similar methods. But the `BiConsumer` uses exactly
         * `<R>` because doing otherwise causes complications with types that can not be expressed in Java
         * (kinds of `<? super ? super R>`). The casts below are okay if we do not invoke any `expression`
         * method with a return value (directly or indirectly as list elements) of exactly `<R>` type.
         * Such methods do not exist in the GeoAPI interfaces, so we are safe if the `BiConsumer` does
         * not invoke implementation-specific methods.
         */
        if (f != null) {
            f.accept(expression, accumulator);
        } else {
            typeNotFound(type, expression, accumulator);
        }
    }

    /**
     * Adds the value to use or throws an exception when there is no action registered for a given filter type.
     * The default implementation throws {@link UnsupportedOperationException}.
     *
     * @param  type         the filter type which has not been found, or {@code null} if {@coce filter} is null.
     * @param  filter       the filter (may be {@code null}).
     * @param  accumulator  where to write the result of all actions.
     * @throws UnsupportedOperationException if there is no default action.
     */
    protected void typeNotFound(final CodeList<?> type, final Filter<R> filter, final A accumulator) {
        throw new UnsupportedOperationException(Resources.format(Resources.Keys.CanNotVisit_2, 0, type));
    }

    /**
     * Adds the value to use or throws an exception when there is no action registered for a given expression type.
     * The default implementation throws {@link UnsupportedOperationException}.
     *
     * @param  type         the expression type which has not been found, or {@code null} if {@coce expression} is null.
     * @param  expression   the expression (may be {@code null}).
     * @param  accumulator  where to write the result of all actions.
     * @throws UnsupportedOperationException if there is no default value.
     */
    protected void typeNotFound(final String type, final Expression<R,?> expression, final A accumulator) {
        throw new UnsupportedOperationException(Resources.format(Resources.Keys.CanNotVisit_2, 1, type));
    }
}
