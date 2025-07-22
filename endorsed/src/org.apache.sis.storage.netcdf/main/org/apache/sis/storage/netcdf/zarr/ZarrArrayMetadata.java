package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.sis.storage.netcdf.base.DataType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the metadata for a Zarr array.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class ZarrArrayMetadata extends ZarrNodeMetadata {

    private ZarrArrayMetadata() {
        super(null, null, null);
        this.nodeType = "array";
    }

    public ZarrArrayMetadata(String name, Path path, Map<String, Object> attributes, DataType dataType,
                             int[] shape, int[] chunkShape, String[] dimensionNames, Object fillValue) {
        super(name, path, attributes);
        this.nodeType = "array";
        this.setDataType(dataType);

        this.chunkKeyEncoding = new ChunkKeyEncoding();
        this.chunkKeyEncoding.setName("default");
        this.chunkKeyEncoding.setConfiguration(Map.of("separator", "/"));

        this.chunkGrid = new ChunkGrid();
        this.chunkGrid.setName("regular");
        this.chunkGrid.setConfiguration(new ChunkGrid.Configuration(chunkShape));
        
        this.shape = shape;
        this.dimensionNames = dimensionNames;

        this.arrayDataPath = path.resolve("c");
        this.fillValue = fillValue;

        this.storageTransformers = List.of(); // Not supported yet

        this.codecs = new ArrayList<>();
        if (dataType == DataType.STRING) {
            this.codecs.add(new VlenUtf8Codec(Map.of()));
        } else {
            this.codecs.add(new BytesCodec(Map.of("endian", "little")));
        }
        this.codecs.add(new ZstdCodec(Map.of("level", 0, "checksum", false)));
    }

    /**
     * The path to the array data in the filesystem.
     * This path points to the directory where the array's chunk data is stored.
     */
    @JsonIgnore
    private Path arrayDataPath;

    /**
     * The shape of the Zarr array.
     */
    @JsonProperty("shape")
    private int[] shape;

    /**
     * The data type of the Zarr array.
     */
    @JsonProperty("data_type")
    private String dataType;

    /**
     * The names of the dimensions of the Zarr array.
     */
    @JsonProperty("dimension_names")
    private String[] dimensionNames;

    /**
     * The chunk grid configuration for the Zarr array.
     * This defines how the array is divided into chunks.
     */
    @JsonProperty("chunk_grid")
    private ChunkGrid chunkGrid;

    /**
     * The encoding used for chunk keys in the Zarr array.
     * This defines how the chunk keys are represented in the filesystem.
     */
    @JsonProperty("chunk_key_encoding")
    private ChunkKeyEncoding chunkKeyEncoding;

    /**
     * The fill value for the Zarr array.
     * This is used to fill in missing data in the array.
     */
    @JsonProperty("fill_value")
    private Object fillValue;

    /**
     * The list of codecs used for encoding/decoding the Zarr array data.
     */
    @JsonIgnore
    public List<AbstractZarrCodec> codecs;

    /**
     * The list of storage transformers applied to the Zarr array data.
     * These transformers can be used to modify the data before or after storage.
     *
     * Not Supported Yet
     */
    @JsonProperty("storage_transformers")
    private List<Object> storageTransformers;

    /**
     * The list of representation types for the Zarr array.
     * This list is computed based on the codecs defined for the array.
     * See {@link #computeDecodedRepresentationList()} for details.
     */
    @JsonIgnore
    private List<ZarrRepresentationType> representationTypes;

    /**
     * Get the path to the chunk data for a given chunk key.
     * Example:
     * If the array data path is "/data/my_array", the chunk key (indices) is [0, 1, 2], and the encoding use '/' as a separator,
     * the resulting path will be : /data/my_array/c/0/1/2
     *
     * @param chunkKey the chunk key as an array of integers, representing the indices of the chunk in the grid.
     * @return the path to the chunk data.
     */
    public Path getChunkPath(int[] chunkKey) {
        if (chunkGrid == null || chunkGrid.configuration() == null) {
            throw new IllegalStateException("Chunk grid configuration is not set");
        }
        if (chunkKeyEncoding == null || chunkKeyEncoding.configuration() == null) {
            throw new IllegalStateException("Chunk key encoding is not set");
        }

        String separator = chunkKeyEncoding.configuration().getOrDefault("separator", "/").toString();

        StringBuilder pathBuilder = new StringBuilder();
        for (int key : chunkKey) {
            pathBuilder.append(key).append(separator);
        }

        return arrayDataPath.resolve(pathBuilder.toString());
    }

    /**
     * Calculates the shape of the grid of chunks based on the array shape and chunk shape.
     * This method assumes that the chunk shape is defined in the chunk grid configuration.
     * *Example:*
     * If the array shape is [100, 200] and the chunk shape is [10, 20],
     * the resulting grid shape will be [10, 10].
     *
     * @return an array representing the shape of the grid of chunks.
     */
    @JsonIgnore
    public int[] getChunksGridShape() {
        int[] shape = shape();
        int[] chunkShape;
        if (chunkGrid != null && chunkGrid.configuration() != null) {
            chunkShape = chunkGrid.configuration().chunkShape();
        } else {
            return new int[0]; // Default to empty array if no chunk shape is defined
        }

        int max = Math.max(shape.length, chunkShape.length);
        int[] gridShape = new int[max];

        for (int i = 0; i < max; i++) {
            int s = (i < shape.length) ? shape[i] : 1;          // pad with 1 if shape shorter
            int c = (i < chunkShape.length) ? chunkShape[i] : 1; // pad with 1 if chunkShape shorter
            gridShape[i] = (int) Math.ceil((double) s / c);
        }
        return gridShape;
    }


    /**
     * Computes the list of decoded representation types for this Zarr array metadata.
     * This method iterates through the defined codecs and computes the representation types
     * The output list is sorted in the "encoded" order, meaning that the first element is the base
     * representation type (array) and the last element is the data stored (bytes).
     * To decode the data, you can use this list in reverse order.
     *
     * @return a list of {@link ZarrRepresentationType} representing the decoded types.
     */
    private List<ZarrRepresentationType> computeDecodedRepresentationList() {
        if (codecs == null || codecs.isEmpty()) {
            throw new IllegalStateException("No codecs defined for this Zarr array metadata");
        }

        List<ZarrRepresentationType> representations = new ArrayList<>();
        representations.add(ZarrRepresentationType.array(chunkGrid.configuration().chunkShape(), dataType())); // Add the base representation type

        int i = 0;
        for (AbstractZarrCodec codec : codecs) {
            ZarrRepresentationType decodedType = codec.computeEncodedType(representations.get(i));
            if (decodedType != null) {
                representations.add(decodedType);
            }
            i++;
        }
        return representations;
    }

    // Getters and Setters

    /**
     * Returns the data type of the Zarr array.
     * @return the data type as a {@link DataType} enum value.
     */
    public DataType dataType() {
        return DataType.zarrValueOf(dataType);
    }

    /**
     * Sets the data type of the Zarr array.
     * @param dataType the data type as a string.
     */
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    /**
     * Sets the data type of the Zarr array.
     * @param dataType the data type
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType.zarrName;
    }

    /**
     * Returns the names of the dimensions of the Zarr array.
     * @return an array of dimension names.
     */
    public String[] dimensionNames() {
        return dimensionNames;
    }

    /**
     * Sets the names of the dimensions of the Zarr array.
     * @param dimensionNames an array of dimension names.
     */
    public void setDimensionNames(String[] dimensionNames) {
        this.dimensionNames = dimensionNames;
    }

    /**
     * Returns the path to the array data in the filesystem.
     * @return the path to the array data.
     */
    public Path arrayDataPath() {
        return arrayDataPath;
    }

    /**
     * Sets the path to the array data in the filesystem.
     * @param arrayDataPath the path to the array data.
     */
    public void setArrayDataPath(Path arrayDataPath) {
        this.arrayDataPath = arrayDataPath;
    }

    /**
     * Returns the shape of the Zarr array.
     * @return an array representing the shape of the Zarr array.
     */
    public int[] shape() {
        return shape;
    }

    /**
     * Sets the shape of the Zarr array.
     * @param shape an array representing the shape of the Zarr array.
     */
    public void setShape(int[] shape) {
        this.shape = shape;
    }

    /**
     * Returns the chunk grid configuration for the Zarr array.
     * @return the chunk grid configuration.
     */
    public ChunkGrid chunkGrid() {
        return chunkGrid;
    }

    /**
     * Sets the chunk grid configuration for the Zarr array.
     * @param chunkGrid the chunk grid configuration.
     */
    public void setChunkGrid(ChunkGrid chunkGrid) {
        this.chunkGrid = chunkGrid;
    }

    /**
     * Returns the encoding used for chunk keys in the Zarr array.
     * @return the chunk key encoding.
     */
    public ChunkKeyEncoding chunkKeyEncoding() {
        return chunkKeyEncoding;
    }

    /**
     * Sets the encoding used for chunk keys in the Zarr array.
     * @param chunkKeyEncoding the chunk key encoding.
     */
    public void setChunkKeyEncoding(ChunkKeyEncoding chunkKeyEncoding) {
        this.chunkKeyEncoding = chunkKeyEncoding;
    }

    /**
     * Returns the fill value for the Zarr array.
     * This is used to fill in missing data in the array.
     * @return the fill value.
     */
    public Object fillValue() {
        return fillValue;
    }

    /**
     * Sets the fill value for the Zarr array.
     * This is used to fill in missing data in the array.
     * @param fillValue the fill value.
     */
    public void setFillValue(Object fillValue) {
        this.fillValue = fillValue;
    }

    /**
     * Returns the list of codecs used for encoding/decoding the Zarr array data.
     * @return a list of {@link AbstractZarrCodec} instances.
     */
    public List<AbstractZarrCodec> codecs() {
        return codecs;
    }

    /**
     * Sets the list of codecs used for encoding/decoding the Zarr array data.
     * @param codecs a list of {@link AbstractZarrCodec} instances.
     */
    public void setCodecs(List<AbstractZarrCodec> codecs) {
        this.codecs = codecs;
    }

    /**
     * Returns the list of storage transformers applied to the Zarr array data.
     * @return a list of storage transformers.
     */
    public List<Object> storageTransformers() {
        return storageTransformers;
    }

    /**
     * Sets the list of storage transformers applied to the Zarr array data.
     * @param storageTransformers a list of storage transformers.
     */
    public void setStorageTransformers(List<Object> storageTransformers) {
        this.storageTransformers = storageTransformers;
    }

    /**
     * Returns the list of representation types for the Zarr array.
     * This list is computed based on the codecs defined for the array.
     * See {@link #computeDecodedRepresentationList()} for details.
     *
     * @return a list of {@link ZarrRepresentationType} representing the representation types.
     */
    public List<ZarrRepresentationType> representationTypes() {
        if (representationTypes == null) {
            representationTypes = computeDecodedRepresentationList();
        }
        return representationTypes;
    }

    /**
     * Class representing the chunk grid configuration for a Zarr array.
     */
    public static final class ChunkGrid {

        /**
         * Name of the chunk grid configuration.
         */
        @JsonProperty("name")
        private String name;

        /**
         * Configuration for the chunk grid, including chunk shape.
         */
        @JsonProperty("configuration")
        private Configuration configuration;

        /**
         * Returns the chunk grid configuration.
         * @return the configuration for the chunk grid.
         */
        public Configuration configuration() {
            return configuration;
        }

        /**
         * Sets the chunk grid configuration.
         * @param configuration the configuration for the chunk grid.
         */
        public void setConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }

        /**
         * Returns the name of the chunk grid configuration.
         * @return the name of the chunk grid.
         */
        public String name() {
            return name;
        }

        /**
         * Sets the name of the chunk grid configuration.
         * @param name the name of the chunk grid.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Class representing the configuration for the chunk grid.
         * This includes the shape of each chunk in the Zarr array.
         */
        public static class Configuration {

            private Configuration() {}

            public Configuration(int[] chunkShape) {
                this.chunkShape = chunkShape;
            }

            /**
             * Shape of one chunk in the Zarr array.
             * This is an array of integers representing the dimensions of the chunk.
             */
            @JsonProperty("chunk_shape")
            private int[] chunkShape;

            /*
             * Shape of one chunk in the Zarr array
             *
             * Even when a chunk is located at the edge of the array and doesn't fully fit within the array bounds,
             * the chunking shape you specify (e.g., (5, 5)) is still valid.
             *
             * Chunks at the border of an array always have the full chunk size, even when the array only covers parts of it.
             * For example, having an array with "shape": [30, 30] and "chunk_shape": [16, 16], the chunk 0,1 would also contain
             * unused values for the indices 0-16, 30-31. When writing such chunks it is recommended to use the current fill
             * value for elements outside the bounds of the array.
             */
            public int[] chunkShape() {
                return chunkShape;
            }

            /**
             * Sets the shape of one chunk in the Zarr array.
             * @param chunkShape an array of integers representing the dimensions of the chunk.
             */
            public void setChunkShape(int[] chunkShape) {
                this.chunkShape = chunkShape;
            }
        }
    }

    /**
     * Class representing the encoding used for chunk keys in a Zarr array.
     */
    public static final class ChunkKeyEncoding {

        /**
         * Name of the chunk key encoding.
         */
        @JsonProperty("name")
        private String name;

        /**
         * Configuration for the chunk key encoding, including separator and other parameters.
         */
        @JsonProperty("configuration")
        private Map<String, Object> configuration;

        /**
         * Returns the configuration for the chunk key encoding.
         * @return a map containing the configuration parameters.
         */
        public Map<String, Object> configuration() {
            return configuration;
        }

        /**
         * Sets the configuration for the chunk key encoding.
         * @param configuration a map containing the configuration parameters.
         */
        public void setConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration;
        }

        /**
         * Returns the name of the chunk key encoding.
         * @return the name of the chunk key encoding.
         */
        public String name() {
            return name;
        }

        /**
         * Sets the name of the chunk key encoding.
         * @param name the name of the chunk key encoding.
         */
        public void setName(String name) {
            this.name = name;
        }
    }
}


