package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.netcdf.base.DataType;

/**
 * Represents the type of data in a Zarr representation.
 * This class encapsulates the shape and data type of arrays,
 * or indicates that the representation is a byte array.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class ZarrRepresentationType {

    /**
     * The shape of the array, or null if this is a byte representation.
     */
    private final int[] shape;   // null for bytes

    /**
     * The data type of the elements in the array, or null if this is a byte representation.
     */
    private final DataType dtype; // null for bytes

    /**
     * Indicates whether this representation is a byte array.
     * If true, the shape and dtype are not applicable.
     */
    private final boolean isBytes;
    // Add more if needed (memory layout, etc.)

    /**
     * Creates a Zarr representation type for an array with the specified shape and data type.
     *
     * @param shape the shape of the array, or null if this is a byte representation
     * @param dtype the data type of the elements in the array, or null if this is a byte representation
     * @return a new ZarrRepresentationType instance representing an array
     */
    public static ZarrRepresentationType array(int[] shape, DataType dtype) {
        return new ZarrRepresentationType(shape, dtype, false);
    }

    /**
     * Creates a Zarr representation type for a byte array.
     * This indicates that the representation is a byte array,
     * and the shape and data type are not applicable.
     *
     * @return a new ZarrRepresentationType instance representing bytes
     */
    public static ZarrRepresentationType bytes() {
        return new ZarrRepresentationType(null, null, true);
    }

    /**
     * Private Constructor
     */
    private ZarrRepresentationType(int[] shape, DataType dtype, boolean isBytes) {
        this.shape = shape != null ? shape.clone() : null;
        this.dtype = dtype;
        this.isBytes = isBytes;
    }

    /**
     * Returns whether this representation is a byte array.
     * @return true if this representation is bytes, false if it is an array
     */
    public boolean isBytes() {
        return isBytes;
    }

    /**
     * Returns the data type of the elements in the array.
     * This is null if this representation is a byte array.
     * @return the data type of the elements, or null if this is bytes
     */
    public DataType dtype() {
        return dtype;
    }

    /**
     * Returns the shape of the array.
     * This is null if this representation is a byte array.
     * @return the shape of the array, or null if this is bytes
     */
    public int[] shape() {
        return shape;
    }
}
