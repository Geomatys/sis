/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sis.storage.netcdf.zarr.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.nio.file.Path;
import java.util.Map;

/**
 *
 * @author  Quentin Bialota (Geomatys)
 */
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
