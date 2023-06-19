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
package org.apache.sis.internal.style;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlRootElement;


/**
 * Mapping of fixed-numeric pixel values to colors.
 * It can be used for defining the colors of a palette-type raster source.
 * For example, a DEM raster giving elevations in meters above sea level
 * can be translated to a colored image.
 *
 * <p>This is a placeholder for future development.
 * OGC 05-077r4 standard defines the dependent classes,
 * but there is too many of them for this initial draft.</p>
 *
 * @author  Johann Sorel (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 1.5
 * @since   1.5
 */
@XmlType(name = "ColorMapType", propOrder = {
//  "categorize",
//  "interpolate",
//  "jenks"
})
@XmlRootElement(name = "ColorMap")
public class ColorMap extends StyleElement {
    /**
     * Creates a color map.
     */
    public ColorMap() {
    }

    /**
     * Creates a shallow copy of the given object.
     * For a deep copy, see {@link #clone()} instead.
     *
     * @param  source  the object to copy.
     */
    public ColorMap(final ColorMap source) {
        super(source);
    }

    /**
     * Returns all properties contained in this class.
     * This is used for {@link #equals(Object)} and {@link #hashCode()} implementations.
     */
    @Override
    final Object[] properties() {
        return new Object[] {};
    }

    /**
     * Returns a deep clone of this object. All style elements are cloned,
     * but expressions are not on the assumption that they are immutable.
     *
     * @return deep clone of all style elements.
     */
    @Override
    public ColorMap clone() {
        final var clone = (ColorMap) super.clone();
        return clone;
    }
}
