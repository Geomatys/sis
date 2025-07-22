package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata for a Zarr group.
 * A Zarr group can contain other groups or arrays.
 *
 * @author  Quentin Bialota (Geomatys)
 */
final class ZarrGroupMetadata extends ZarrNodeMetadata {

    private ZarrGroupMetadata() {
        super(null, null, null);
        this.nodeType = "group";
    }

    public ZarrGroupMetadata(String name, Path path, Map<String, Object> attributes) {
        super(name, path, attributes);
        this.nodeType = "group";
    }

    /*
     * The consolidated metadata for the group, and for all its child nodes.
     * Not used in this implementation, because some Zarr datasets may not have consolidated metadata.
     */
    @JsonProperty("consolidated_metadata")
    private Map<String, Object> consolidatedMetadata;

    /**
     * Map of child node metadata.
     * Keys are child node names, values are their corresponding ZarrNodeMetadata.
     */
    @JsonIgnore
    private final Map<String, ZarrNodeMetadata> children = new HashMap<>();

    /**
     * Finds a child node metadata by its name.
     * @param name the name of the child node to find
     * @return the ZarrNodeMetadata of the child node, or null if not found
     */
    public ZarrNodeMetadata findChildNodeMetadata(String name) {
        return children.get(name);
    }

    /**
     * Adds a child node metadata to the group.
     * @param name the name of the child node to add
     * @param node the ZarrNodeMetadata of the child node to add
     */
    public void addChildNodeMetadata(String name, ZarrNodeMetadata node) {
        if (children.containsKey(name)) {
            throw new IllegalArgumentException("Node with name '" + name + "' already exists in the group.");
        }
        children.put(name, node);
    }

    /**
     * Returns the map of child node metadata.
     * @return a map where keys are child node names and values are their corresponding ZarrNodeMetadata
     */
    @JsonIgnore
    public Map<String, ZarrNodeMetadata> getChildrenNodeMetadata() {
        return children;
    }

    /**
     * Returns the consolidated metadata for the group.
     * @return a map of consolidated metadata, or null if not available
     */
    public Map<String, Object> getConsolidatedMetadata() {
        return consolidatedMetadata;
    }
}


