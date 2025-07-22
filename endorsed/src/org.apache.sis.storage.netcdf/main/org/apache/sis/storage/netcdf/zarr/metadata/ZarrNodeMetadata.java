package org.apache.sis.storage.netcdf.zarr.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.nio.file.Path;
import java.util.Map;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "node_type",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ZarrArrayMetadata.class, name = "array"),
        @JsonSubTypes.Type(value = ZarrGroupMetadata.class, name = "group")
})
public abstract class ZarrNodeMetadata {

    @JsonIgnore
    protected String name;

    /**
     * The path to the Zarr node in the filesystem.
     * Example: "/path/to/zarr/root/my_group/my_array"
     */
    @JsonIgnore
    protected Path path;

    /**
     * The path to the Zarr node in the Zarr dataset.
     * This is used for serialization and should not be confused with the filesystem path.
     * Example: "root/my_group/my_array"
     */
    @JsonIgnore
    protected String zarrPath;

    @JsonProperty("zarr_format")
    protected int zarrFormat;

    @JsonProperty("node_type")
    protected String nodeType;

    @JsonProperty("attributes")
    protected Map<String, Object> attributes;

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Path path() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public String zarrPath() {
        return zarrPath;
    }

    public void setZarrPath(String zarrPath) {
        this.zarrPath = zarrPath;
    }

    public int zarrFormat() {
        return zarrFormat;
    }

    public void setZarrFormat(int zarrFormat) {
        this.zarrFormat = zarrFormat;
    }

    public String nodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
