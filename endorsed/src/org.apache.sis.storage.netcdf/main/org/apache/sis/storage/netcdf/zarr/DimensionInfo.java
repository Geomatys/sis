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

import org.apache.sis.storage.netcdf.base.Dimension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A dimension in a Zarr array. A dimension can be seen as an axis in the grid space
 * (not the geodetic space). {@code Dimension} instances are unique, i.e. {@code a == b}
 * is a sufficient test for determining that {@code a} and {@code b} represent the same
 * dimension.
 *
 * @author  Quentin Bialota (Geomatys)
 * @author  Johann Sorel (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 */
final class DimensionInfo extends Dimension {
    /**
     * The dimension name.
     */
    final String name;

    /**
     * The Zarr path of this dimension.
     */
    final String zarrPath;

    /**
     * The number of grid cell value along this dimension, as an unsigned number.
     */
    final int length;

    /**
     * The paths to the arrays that are associated with this dimension.
     */
    final List<String> arrayPaths;

    /**
     * The names of the source arrays that are associated with this dimensions.
     */
    final Map<String, String[]> sourceNames;

    /**
     * Whether this dimension is the "record" (also known as "unlimited") dimension.
     * There is at most one record dimension in a well-formed netCDF file.
     */
    final boolean isUnlimited = false; // Zarr does not have unlimited dimensions, but we keep this field for compatibility with Dimension class.

    /**
     * Creates a new dimension of the given name and length.
     *
     * @param name         the dimension name.
     * @param length       the number of grid cell value along this dimension, as an unsigned number.
     */
    public DimensionInfo(final String name, final int length) {
        this.name        = name;
        this.length      = length;
        this.zarrPath   = null;
        this.arrayPaths = new ArrayList<>();
        this.sourceNames = new HashMap<>();
    }

    /**
     * Creates a new dimension of the given name and length.
     *
     * @param name the dimension name.
     * @param length      the number of grid cell value along this dimension, as an unsigned number.
     * @param zarrPath   the Zarr path of this dimension.
     */
    public DimensionInfo(final String name, final int length, final String zarrPath) {
        this.name        = name;
        this.length      = length;
        this.zarrPath   = zarrPath;
        this.arrayPaths = new ArrayList<>();
        this.sourceNames = new HashMap<>();
    }

    /**
     * Returns the name of this netCDF dimension.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the Zarr path of this dimension.
     */
    public String getZarrPath() {
        return zarrPath;
    }

    /**
     * Returns the number of grid cell value along this dimension.
     * In this implementation, the length is never undetermined (negative).
     */
    @Override
    public long length() {
        return Integer.toUnsignedLong(length);
    }

    /**
     * Returns whether this dimension can grow.
     */
    @Override
    protected boolean isUnlimited() {
        return isUnlimited;
    }

    /**
     * Returns if this dimension is used in the specified array path.
     *
     * @param arrayPath the path to the array to check.
     * @return {@code true} if this dimension is used in the specified array path,
     */
    public boolean isDimensionUsedInArray(String arrayPath) {
        if (arrayPaths.contains(arrayPath)) {
            return true;
//            String[] sources = sourceNames.get(arrayPath);
//
//            if (sources != null) {
//                for (String source : sources) {
//                    if (source.equals(name)) {
//                        return true;
//                    }
//                }
//            } else {
//                if (length() > 0) {}
//                return true;
//            }

        }
        return false;
    }

    public void addArrayPath(String arrayPath, String[] sourceNames) {
        if (!arrayPaths.contains(arrayPath)) {
            arrayPaths.add(arrayPath);
        }
        if (sourceNames != null && sourceNames.length > 0) {
            this.sourceNames.put(arrayPath, sourceNames);
        } else {
            this.sourceNames.put(arrayPath, null);
        }
    }

    @Override
    public String toString() {
        return name + "[" + Integer.toUnsignedLong(length) + "] (" + zarrPath + ")";
    }
}
