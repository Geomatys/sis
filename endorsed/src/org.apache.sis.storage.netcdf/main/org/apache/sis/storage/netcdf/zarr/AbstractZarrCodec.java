package org.apache.sis.storage.netcdf.zarr;

import java.util.Map;

/**
 * Abstract base class for Zarr codecs, providing common functionality
 *
 * @author  Quentin Bialota (Geomatys)
 */
abstract class AbstractZarrCodec implements ZarrCodecInterface {

    /**
     * The Zarr codec type.
     */
    protected final ZarrCodec codec;

    /**
     * Configuration parameters for the codec.
     * This can include compression levels, encoding options, etc.
     */
    protected final Map<String, Object> configuration;

    /**
     * Constructor for AbstractZarrCodec.
     *
     * @param codec the Zarr codec type
     * @param configuration the configuration parameters for the codec
     */
    protected AbstractZarrCodec(ZarrCodec codec, Map<String, Object> configuration) {
        this.codec = codec;
        this.configuration = configuration;
    }

    /**
     * Returns the Zarr codec type.
     * @return the Zarr codec type
     */
    public ZarrCodec codec() {
        return codec;
    }

    /**
     * Returns a specific configuration parameter by key.
     * @return the configuration parameter value, or null if not found
     */
    public Object getConfiguration(String key) {
        if (configuration != null) {
            return configuration.get(key);
        }
        return null;
    }
}
