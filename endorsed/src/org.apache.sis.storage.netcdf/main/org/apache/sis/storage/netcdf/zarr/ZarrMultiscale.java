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
package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Represents the "multiscales" metadata attribute in a Zarr group.
 * This structure describes the hierarchical layout of Zarr groups representing
 * different resolution levels.
 *
 * @see <a href="https://github.com/zarr-conventions/multiscales">Multiscales
 *      Attribute Extension for Zarr</a>
 *
 * @author Quentin Bialota (Geomatys)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final class ZarrMultiscale {

    /**
     * Array of objects representing the pyramid layout.
     */
    @JsonProperty(value = "layout", required = true)
    public List<Level> layout;

    /**
     * Default resampling method used for resampling data (optional).
     */
    @JsonProperty("resampling_method")
    public String resamplingMethod;

    /**
     * Represents a single resolution level in the multiscale pyramid.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class Level {
        /**
         * Path to the Zarr group or array for this resolution level.
         */
        @JsonProperty(value = "asset", required = true)
        public String asset;

        /**
         * Path to the source Zarr group or array used to generate this level.
         */
        @JsonProperty("derived_from")
        public String derivedFrom;

        /**
         * Transformation parameters describing the coordinate transformation for this
         * level.
         * Required if "derived_from" is present.
         * This object contains RELATIVE transform parameters (from one level to the next).
         */
        @JsonProperty("transform")
        public Transform transform;

        /**
         * Resampling method for this specific level.
         */
        @JsonProperty("resampling_method")
        public String resamplingMethod;

        /**
         * Affine transformation matrix (6 parameters) describing absolute position.
         * This object contains ABSOLUTE transform parameters (positioning in the coordinate space).
         */
        @JsonProperty("spatial:transform")
        public double[] spatialTransform;

        /**
         * 	Shape of the raster in pixels [height, width].
         * 	This object contains ABSOLUTE shape parameters (positioning in the coordinate space).
         */
        @JsonProperty("spatial:shape")
        public int[] spatialShape;
    }

    /**
     * Describes the coordinate transformation between resolution levels.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class Transform {
        /**
         * Array of scale factors per axis.
         */
        @JsonProperty("scale")
        public double[] scale;

        /**
         * Array of translation offsets per axis.
         */
        @JsonProperty("translation")
        public double[] translation;
    }
}
