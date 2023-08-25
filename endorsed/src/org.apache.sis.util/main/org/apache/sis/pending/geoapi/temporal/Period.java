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
package org.apache.sis.pending.geoapi.temporal;

// Specific to the main branch:
import org.opengis.temporal.TemporalPrimitive;


/**
 * Placeholder for a GeoAPI interfaces not present in GeoAPI 3.0.
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @since   0.3
 * @version 0.3
 */
public interface Period extends TemporalPrimitive {
    /**
     * Links this period to the instant at which it ends.
     *
     * @return The beginning instant.
     */
    Instant getBeginning();

    /**
     * Links this period to the instant at which it ends.
     *
     * @return The end instant.
     */
    Instant getEnding();
}
