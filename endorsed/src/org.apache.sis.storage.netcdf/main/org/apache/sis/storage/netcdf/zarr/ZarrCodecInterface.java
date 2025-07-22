package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.storage.DataStoreException;

/**
 * Interface for Zarr codecs, defining methods for encoding and decoding data.
 * This interface is used to implement various Zarr codecs that handle data compression
 * and encoding in Zarr datasets.
 *
 * @author  Quentin Bialota (Geomatys)
 */
interface ZarrCodecInterface {
    /**
     * Computes the output representation type, given an input ("decoded") representation type.
     * @param decodedType Array info: shape, data type, etc. (you may want a struct for this!)
     * @return The output type after encoding (e.g. different dtype, shape, etc.; or byte[] for array->bytes)
     */
    ZarrRepresentationType computeEncodedType(ZarrRepresentationType decodedType);

    /**
     * Encodes given decoded value (array or other), producing encoded value (array or bytes)
     */
    Object encode(Object decodedValue, ZarrRepresentationType decodedType) throws DataStoreException;

    /**
     * Decodes encoded value (byte[] for array->bytes, any for bytes->bytes), returning decoded value.
     */
    Object decode(Object encodedValue, ZarrRepresentationType decodedType) throws DataStoreException;
}
