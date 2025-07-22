package org.apache.sis.storage.netcdf.zarr.metadata;

import java.util.Map;

public class ZarrCodecInfo {
    public final ZarrCodec codec;
    public final String originalName;
    public final Map<String, Object> configuration;

    public ZarrCodecInfo(ZarrCodec codec, String originalName, Map<String, Object> configuration) {
        this.codec = codec;
        this.originalName = originalName;
        this.configuration = configuration;
    }
}
