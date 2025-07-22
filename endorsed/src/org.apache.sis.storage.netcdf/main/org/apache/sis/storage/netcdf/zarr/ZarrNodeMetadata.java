package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.nio.file.Path;
import java.util.Map;

/**
 * Abstract base class for Zarr node metadata.
 * This class represents a node in the Zarr metadata tree, which can be either a group or an array.
 * It contains common properties and methods for both types of nodes.
 *
 * @author  Quentin Bialota (Geomatys)
 */
@JsonSubTypes({
        @JsonSubTypes.Type(value = ZarrArrayMetadata.class, name = "array"),
        @JsonSubTypes.Type(value = ZarrGroupMetadata.class, name = "group")
})
abstract class ZarrNodeMetadata {

    protected ZarrNodeMetadata(String name, Path path, Map<String, Object> attributes) {
        this.zarrFormat = 3;
        this.attributes = attributes;
        this.name = name;
        this.path = path;
    }

    /**
     * The name of the Zarr node.
     */
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

    /**
     * The Zarr format version of the node.
     * This indicates the version of the Zarr specification that the node adheres to.
     */
    @JsonProperty("zarr_format")
    protected int zarrFormat;

    /**
     * The type of the Zarr node, which can be either "group" or "array".
     * This is used to differentiate between different types of nodes in the Zarr metadata tree.
     */
    @JsonProperty("node_type")
    protected String nodeType;

    /**
     * A map of attributes associated with the Zarr node.
     * Attributes are key-value pairs that provide additional information about the node.
     *
     * For GeoZarr, common attributes include:
     * - "crs": Coordinate Reference System information
     * - "transform": Affine transformation parameters
     * - "grid_mapping": Reference to a grid mapping variable
     * - "spatial_ref": Spatial reference information
     * - "_ARRAY_DIMENSIONS": Dimensions of the array (for arrays only) (ex ["y", "x"])
     */
    @JsonProperty("attributes")
    protected Map<String, Object> attributes;

    /**
     * Returns the name of the Zarr node.
     * @return the name of the Zarr node
     */
    public String name() {
        return name;
    }

    /**
     * Sets the name of the Zarr node.
     * @param name the name to set for the Zarr node
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the filesystem path to the Zarr node.
     * @return the filesystem path to the Zarr node
     */
    public Path path() {
        return path;
    }

    /**
     * Sets the filesystem path to the Zarr node.
     * @param path the path to set for the Zarr node
     */
    public void setPath(Path path) {
        this.path = path;
    }

    /**
     * Returns the Zarr path to the node in the Zarr dataset.
     * @return the Zarr path to the node
     */
    public String zarrPath() {
        return zarrPath;
    }

    /**
     * Sets the Zarr path to the node in the Zarr dataset.
     * This path is used for serialization and should not be confused with the filesystem path.
     * @param zarrPath the Zarr path to set for the node
     */
    public void setZarrPath(String zarrPath) {
        this.zarrPath = zarrPath;
    }

    /**
     * Returns the Zarr format version of the node.
     * @return the Zarr format version of the node
     */
    public int zarrFormat() {
        return zarrFormat;
    }

    /**
     * Sets the Zarr format version of the node.
     * @param zarrFormat the Zarr format version to set for the node
     */
    public void setZarrFormat(int zarrFormat) {
        this.zarrFormat = zarrFormat;
    }

    /**
     * Returns the type of the Zarr node (either "group" or "array").
     * @return the type of the Zarr node
     */
    @JsonIgnore
    public String nodeType() {
        return nodeType;
    }

    /**
     * Sets the type of the Zarr node.
     * @param nodeType the type to set for the Zarr node (either "group" or "array")
     */
    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    /**
     * Returns the attributes associated with the Zarr node.
     * @return a map of attributes where keys are attribute names and values are their corresponding values
     */
    public Map<String, Object> attributes() {
        return attributes;
    }

    /**
     * Sets the attributes for the Zarr node.
     * @param attributes a map of attributes to set for the Zarr node
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
