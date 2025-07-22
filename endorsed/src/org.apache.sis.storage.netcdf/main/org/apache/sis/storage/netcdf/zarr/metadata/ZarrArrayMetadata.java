package org.apache.sis.storage.netcdf.zarr.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class ZarrArrayMetadata extends ZarrNodeMetadata {

    @JsonIgnore
    protected Path arrayDataPath;

    @JsonProperty("shape")
    private int[] shape;

    @JsonProperty("data_type")
    private String dataType;

    @JsonProperty("dimension_names")
    private String[] dimensionNames;

    @JsonProperty("chunk_grid")
    private ChunkGrid chunkGrid;

    @JsonProperty("chunk_key_encoding")
    private ChunkKeyEncoding chunkKeyEncoding;

    @JsonProperty("fill_value")
    private Object fillValue;

    @JsonIgnore
    public List<ZarrCodecInfo> codecs;

    @JsonProperty("storage_transformers")
    private List<Object> storageTransformers;

    public int[] getChunksGridShape() {
        int[] shape = shape();
        int[] chunkShape;
        if (chunkGrid != null && chunkGrid.configuration() != null) {
            chunkShape = chunkGrid.configuration().chunkShape();
        } else {
            return new int[0]; // Default to empty array if no chunk shape is defined
        }

        if (shape.length != chunkShape.length) {
            throw new IllegalArgumentException("Shape and chunkShape must have the same length");
        }

        int[] gridShape = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            gridShape[i] = (int) Math.ceil((double) shape[i] / chunkShape[i]);
        }
        return gridShape;
    }

    // Getters and Setters

    public String dataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String[] dimensionNames() {
        return dimensionNames;
    }

    public void setDimensionNames(String[] dimensionNames) {
        this.dimensionNames = dimensionNames;
    }

    public Path arrayDataPath() {
        return arrayDataPath;
    }

    public void setArrayDataPath(Path arrayDataPath) {
        this.arrayDataPath = arrayDataPath;
    }

    public int[] shape() {
        return shape;
    }

    public void setShape(int[] shape) {
        this.shape = shape;
    }

    public ChunkGrid chunkGrid() {
        return chunkGrid;
    }

    public void setChunkGrid(ChunkGrid chunkGrid) {
        this.chunkGrid = chunkGrid;
    }

    public ChunkKeyEncoding chunkKeyEncoding() {
        return chunkKeyEncoding;
    }

    public void setChunkKeyEncoding(ChunkKeyEncoding chunkKeyEncoding) {
        this.chunkKeyEncoding = chunkKeyEncoding;
    }

    public Object fillValue() {
        return fillValue;
    }

    public void setFillValue(Object fillValue) {
        this.fillValue = fillValue;
    }

    public List<ZarrCodecInfo> codecs() {
        return codecs;
    }

    public void setCodecs(List<ZarrCodecInfo> codecs) {
        this.codecs = codecs;
    }

    public List<Object> storageTransformers() {
        return storageTransformers;
    }

    public void setStorageTransformers(List<Object> storageTransformers) {
        this.storageTransformers = storageTransformers;
    }

    public static class ChunkGrid {

        @JsonProperty("name")
        private String name;

        @JsonProperty("configuration")
        private Configuration configuration;

        public Configuration configuration() {
            return configuration;
        }

        public void setConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }

        public String name() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public static class Configuration {

            @JsonProperty("chunk_shape")
            private int[] chunkShape;

            public int[] chunkShape() {
                return chunkShape;
            }

            public void setChunkShape(int[] chunkShape) {
                this.chunkShape = chunkShape;
            }
        }
    }

    public static class ChunkKeyEncoding {

        @JsonProperty("name")
        private String name;

        @JsonProperty("configuration")
        private Map<String, Object> configuration;

        public Map<String, Object> configuration() {
            return configuration;
        }

        public void setConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration;
        }

        public String name() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}


