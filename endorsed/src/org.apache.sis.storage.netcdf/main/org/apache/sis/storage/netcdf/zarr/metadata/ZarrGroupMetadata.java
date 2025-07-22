package org.apache.sis.storage.netcdf.zarr.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class ZarrGroupMetadata extends ZarrNodeMetadata {

    @JsonProperty("consolidated_metadata")
    protected String consolidatedMetadata;

    @JsonIgnore
    private Map<String, ZarrNodeMetadata> children = new HashMap<>();

    public ZarrNodeMetadata findChildNodeMetadata(String name) {
        return children.get(name);
    }

    public void addChildNodeMetadata(String name, ZarrNodeMetadata node) {
        if (children.containsKey(name)) {
            throw new IllegalArgumentException("Node with name '" + name + "' already exists in the group.");
        }
        children.put(name, node);
    }

    public Map<String, ZarrNodeMetadata> getChildrenNodeMetadata() {
        return  children;
    }
}


