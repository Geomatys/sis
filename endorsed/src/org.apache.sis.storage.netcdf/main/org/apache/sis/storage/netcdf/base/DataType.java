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
package org.apache.sis.storage.netcdf.base;

import java.awt.image.DataBuffer;
import org.apache.sis.math.Vector;
import org.apache.sis.util.Numbers;


/**
 * The netCDF type of data. Number of bits and endianness are same as in the Java language
 * except {@link #CHAR}, which is defined as an unsigned 8-bits value. This enumeration is
 * related to the netCDF standard as below:
 *
 * <ul>
 *   <li>The netCDF numerical code is the {@link #ordinal()}.</li>
 *   <li>The CDL reserved word is the {@link #name()} is lower case,
 *       except for {@link #UNKNOWN} which is not a valid CDL type.</li>
 * </ul>
 *
 * The unsigned data types are not defined in netCDF classical version. However, those data types
 * can be inferred from their signed counterpart if the latter have a {@code "_Unsigned = true"}
 * attribute associated to the variable.
 *
 * @author  Johann Sorel (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @author  Quentin Bialota (Geomatys)
 */
public enum DataType {
    /**
     * The enumeration for unknown data type. This is not a valid netCDF type.
     */
    UNKNOWN(Numbers.OTHER, Object.class, false, false, (byte) 0, null, null, true, true),

    /**
     * 8 bits signed integer (netCDF type 1) (zarr name "int8").
     * Can be made unsigned by assigning the “_Unsigned” attribute to a netCDF variable.
     */
    BYTE(Numbers.BYTE, Byte.class, true, false, (byte) 7, org.apache.sis.image.DataType.BYTE, "int8", true, true),

    /**
     * Character type as unsigned 8 bits (netCDF type 2).
     * Encoding can be specified by assigning the “_Encoding” attribute to a netCDF variable.
     */
    CHAR(Numbers.BYTE, Character.class, false, true, (byte) 2, null, null, false, true),   // NOT Numbers.CHARACTER

    /**
     * 16 bits signed integer (netCDF type 3) (zarr name "int16").
     */
    SHORT(Numbers.SHORT, Short.class, true, false, (byte) 8, org.apache.sis.image.DataType.SHORT, "int16", true, true),

    /**
     * 32 bits signed integer (netCDF type 4) (zarr name "int32").
     * This is also called "long", but that name is deprecated.
     */
    INT(Numbers.INTEGER, Integer.class, true, false, (byte) 9, org.apache.sis.image.DataType.INT, "int32", true, true),

    /**
     * 32 bits floating point number (netCDF type 5)
     * This is also called "real".
     */
    FLOAT(Numbers.FLOAT, Float.class, false, false, (byte) 5, org.apache.sis.image.DataType.FLOAT, "float32", true, true),

    /**
     * 64 bits floating point number (netCDF type 6)
     */
    DOUBLE(Numbers.DOUBLE, Double.class, false, false, (byte) 6, org.apache.sis.image.DataType.DOUBLE, "float64", true, true),

    /**
     * 8 bits unsigned integer (netCDF type 7) (zarr name "uint8").
     * Not available in netCDF classic format.
     */
    UBYTE(Numbers.BYTE, Short.class, true, true, (byte) 1, org.apache.sis.image.DataType.BYTE, "uint8", true, true),

    /**
     * 16 bits unsigned integer (netCDF type 8) (zarr name "uint16").
     * Not available in netCDF classic format.
     */
    USHORT(Numbers.SHORT, Integer.class, true, true, (byte) 3, org.apache.sis.image.DataType.USHORT, "uint16", true, true),

    /**
     * 32 bits unsigned integer (netCDF type 9) (zarr name "uint32").
     * Not available in netCDF classic format.
     */
    UINT(Numbers.INTEGER, Long.class, true, true, (byte) 4, org.apache.sis.image.DataType.UINT, "uint32", true, true),

    /**
     * 64 bits signed integer (netCDF type 10) (zarr name "int64").
     * Not available in netCDF classic format.
     */
    INT64(Numbers.LONG, Long.class, true, false, (byte) 11, null, "int64", true, true),

    /**
     * 64 bits unsigned integer (netCDF type 11) (zarr name "uint64").
     * Not available in netCDF classic format.
     */
    UINT64(Numbers.LONG, Number.class, true, true, (byte) 10, null, "uint64", true, true),

    /**
     * Character string (netCDF type 12).
     * Not available in netCDF classic format.
     */
    STRING(Numbers.OTHER, String.class, false, false, (byte) 12, null, "string", true, true),

    /**
     * Boolean type (zarr name "bool").
     */
    BOOLEAN(Numbers.BOOLEAN, Boolean.class, false, false, (byte) 0, null, "bool", true, false),

    /**
     * Complex number with 32 bits for each of the real and imaginary parts (zarr name "complex64").
     */
    COMPLEX64(Numbers.OTHER, Float[].class, false, false, (byte) 0, null, "complex64", true, false),

    /**
     * Complex number with 64 bits for each of the real and imaginary parts (zarr name "complex128").
     */
    COMPLEX128(Numbers.OTHER, Double[].class, false, false, (byte) 0, null, "complex128", true, false),

    /* ======= Zarr raw bit type: "r*" (r8, r16, ...) — handled with special match ======== */
    RAW(Numbers.OTHER, byte[].class, false, false, (byte) 0, null, "raw", true, false);

    /**
     * Mapping from the netCDF data type to the enumeration used by our {@link Numbers} class.
     */
    public final byte number;

    /**
     * {@code true} for data type that are signed or unsigned integers.
     */
    public final boolean isInteger;

    /**
     * {@code false} for signed data type (the default), or {@code true} for unsigned data type.
     * The OGC netCDF standard version 1.0 does not define unsigned data types. However, some data
     * providers attach an {@code "_Unsigned = true"} attribute to the variable.
     */
    public final boolean isUnsigned;

    /**
     * The netCDF code of the data type of opposite sign convention.
     * For example, for the {@link #BYTE} data type, this is the netCDF code of {@link #UBYTE}.
     */
    private final byte opposite;

    /**
     * Wrapper of {@link DataBuffer} constant which most closely represents the "raw" internal data of the variable.
     * This wraps the value to be returned by {@link java.awt.image.SampleModel#getDataType()} for Java2D rasters
     * created from a variable data. If the variable data type cannot be mapped to a Java2D data type, then the
     * raster data type is {@code null}.
     */
    public final org.apache.sis.image.DataType rasterDataType;

    /**
     * Zarr name of the data type, as defined in the Zarr standard.
     * */
    public final String zarrName;

    /**
     * {@code true} if this data type is defined in the Zarr standard. (Also {@code true} for the {@link #UNKNOWN} type.)
     */
    public final boolean isZarrType;

    /**
     * {@code true} if this data type is defined in the netCDF standard. (Also {@code true} for the {@link #UNKNOWN} type.)
     */
    public final boolean isNetCDFType;

    /**
     * The smallest Java wrapper class that can hold the values. Values are always signed. If {@link #isUnsigned}
     * is {@code true}, then a wider type is used for holding the large unsigned values. For example, the 16 bits
     * signed integer type is used for holding 8 bits unsigned integers.
     */
    private final Class<?> classe;

    /**
     * Creates a new enumeration value.
     */
    private DataType(final byte number, final Class<?> classe, final boolean isInteger, final boolean isUnsigned,
            final byte opposite, final org.apache.sis.image.DataType rasterDataType, final String zarrName, final boolean isZarrType, final boolean isNetCDFType)
    {
        this.number         = number;
        this.classe         = classe;
        this.isInteger      = isInteger;
        this.isUnsigned     = isUnsigned;
        this.opposite       = opposite;
        this.rasterDataType = rasterDataType;
        this.zarrName       = zarrName;
        this.isZarrType     = isZarrType;
        this.isNetCDFType   = isNetCDFType;
    }

    /**
     * Returns the Java class to use for storing the values.
     *
     * @param  vector  {@code true} for a vector object, or {@code false} for a scalar object.
     */
    final Class<?> getClass(final boolean vector) {
        if (vector) {
            if (classe == Character.class) {
                return String.class;
            } else if (Number.class.isAssignableFrom(classe)) {
                return Vector.class;
            } else {
                return Object.class;
            }
        } else {
            return classe;
        }
    }

    /**
     * Returns the number of bytes for this data type, or 0 if unknown.
     *
     * @return number of bytes for this data type, or 0 if unknown.
     */
    public final int size() {
        // Complex numbers: size is 8 (two float32) or 16 (two float64)
        if (this == COMPLEX64) return 8;
        if (this == COMPLEX128) return 16;

        switch (number) {
            case Numbers.BYTE:    return Byte.BYTES;
            case Numbers.SHORT:   return Short.BYTES;
            case Numbers.INTEGER: // Same as float
            case Numbers.FLOAT:   return Float.BYTES;
            case Numbers.LONG:    // Same as double
            case Numbers.DOUBLE:  return Double.BYTES;
            default:              return 0;
        }
    }

    /**
     * Returns the signed or unsigned variant of this data type.
     * If this data type does not have the requested variant, then this method returns {@code this}.
     *
     * @param  u  {@code true} for the unsigned variant, or {@code false} for the signed variant.
     * @return the signed or unsigned variant of this data type.
     */
    public final DataType unsigned(final boolean u) {
        return (u == isUnsigned) ? this : valueOf(opposite);
    }

    /**
     * An array of all supported netCDF data types ordered in such a way that
     * {@code VALUES[codeNetCDF - 1]} is the enumeration value for a given netCDF code.
     */
    private static final DataType[] VALUES = values();

    /**
     * Returns the enumeration value for the given netCDF code, or {@link #UNKNOWN} if the given code is unknown.
     *
     * @param  code  the netCDF code.
     * @return enumeration value for the give netCDF code.
     */
    public static DataType valueOf(final int code) {
        return (code >= 0 && code < VALUES.length) ? VALUES[code] : UNKNOWN;
    }

    /**
     * Returns the enumeration value for the given Zarr type name or {@link #UNKNOWN} if unknown.
     * Accepts standard Zarr v3 names ("bool", "uint16", etc.), complex, and raw ("r8", "r16", etc.).
     *
     * @param name the name as defined in Zarr v3 standard (case-insensitive).
     * @return the DataType, or UNKNOWN if not found.
     */
    public static DataType zarrValueOf(final String name) {
        if (name == null) return UNKNOWN;
        final String key = name.trim().toLowerCase();

        // Handle r* (raw bits) types like "r8", "r32" etc.
        if (key.matches("r\\d+")) {
            return RAW;
        }

        for (DataType type : VALUES) {
            if (type.zarrName != null && type.zarrName.equalsIgnoreCase(key)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    /**
     * If this type is RAW, returns the number of bits from its Zarr name (e.g. "r8" -> 8), or -1 if not RAW.
     */
    public static int zarrRawBits(final String name) {
        if (name == null) return -1;
        final String key = name.trim().toLowerCase();
        if (key.matches("r\\d+")) {
            return Integer.parseInt(key.substring(1));
        }
        return -1;
    }
}
