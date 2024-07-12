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
package org.apache.sis.pending.geoapi.referencing;

import java.util.function.Function;
import org.opengis.referencing.crs.*;
import org.opengis.referencing.datum.*;
import org.apache.sis.referencing.crs.DefaultVerticalCRS;
import org.apache.sis.referencing.crs.DefaultTemporalCRS;
import org.apache.sis.referencing.crs.DefaultEngineeringCRS;
import org.apache.sis.referencing.datum.DefaultDatumEnsemble;


/**
 * Placeholder for methods missing in the GeoAPI 3.0 interface.
 */
public final class MissingMethods {
    /**
     * To be set by static {@code AbstractCRS} initializer.
     */
    public static volatile Function<CoordinateReferenceSystem, DefaultDatumEnsemble<?>> datumEnsemble;

    /**
     * To be set by static {@code DefaultGeodeticCRS} initializer.
     */
    public static volatile Function<GeodeticCRS, DefaultDatumEnsemble<GeodeticDatum>> geodeticDatumEnsemble;

    private MissingMethods() {
    }

    /**
     * Returns the datum ensemble of an arbitrary CRS.
     *
     * @param  datum  the CRS from which to get a datum ensemble, or {@code null} if none.
     * @return the datum ensemble, or {@code null} if none.
     */
    public static DefaultDatumEnsemble<?> getDatumEnsemble(final CoordinateReferenceSystem crs) {
        final var m = datumEnsemble;
        return (m != null) ? m.apply(crs) : null;
    }

    /**
     * Returns the datum ensemble of an arbitrary geodetic CRS.
     *
     * @param  datum  the CRS from which to get a datum ensemble, or {@code null} if none.
     * @return the datum ensemble, or {@code null} if none.
     */
    public static DefaultDatumEnsemble<GeodeticDatum> getDatumEnsemble(final GeodeticCRS crs) {
        final var m = geodeticDatumEnsemble;
        return (m != null) ? m.apply(crs) : null;
    }

    /**
     * Returns the datum ensemble of an arbitrary vertical CRS.
     *
     * @param  datum  the CRS from which to get a datum ensemble, or {@code null} if none.
     * @return the datum ensemble, or {@code null} if none.
     */
    public static DefaultDatumEnsemble<VerticalDatum> getDatumEnsemble(final VerticalCRS crs) {
        return (crs instanceof DefaultVerticalCRS) ? ((DefaultVerticalCRS) crs).getDatumEnsemble() : null;
    }

    /**
     * Returns the datum ensemble of an arbitrary temporal CRS.
     *
     * @param  datum  the CRS from which to get a datum ensemble, or {@code null} if none.
     * @return the datum ensemble, or {@code null} if none.
     */
    public static DefaultDatumEnsemble<TemporalDatum> getDatumEnsemble(final TemporalCRS crs) {
        return (crs instanceof DefaultTemporalCRS) ? ((DefaultTemporalCRS) crs).getDatumEnsemble() : null;
    }

    /**
     * Returns the datum ensemble of an arbitrary engineering CRS.
     *
     * @param  datum  the CRS from which to get a datum ensemble, or {@code null} if none.
     * @return the datum ensemble, or {@code null} if none.
     */
    public static DefaultDatumEnsemble<EngineeringDatum> getDatumEnsemble(final EngineeringCRS crs) {
        return (crs instanceof DefaultEngineeringCRS) ? ((DefaultEngineeringCRS) crs).getDatumEnsemble() : null;
    }
}
