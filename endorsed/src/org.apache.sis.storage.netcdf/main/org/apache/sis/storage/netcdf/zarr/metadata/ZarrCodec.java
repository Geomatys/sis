package org.apache.sis.storage.netcdf.zarr.metadata;

public enum ZarrCodec {
    BLOSC,
    BYTES,
    CRC32C,
    GZIP,
    SHARDING_INDEXED,
    TRANSPOSE
}
