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
package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.storage.netcdf.base.DataType;
import org.apache.sis.storage.netcdf.base.Dimension;
import org.apache.sis.storage.netcdf.base.Encoder;
import org.apache.sis.storage.netcdf.base.Variable;
import org.apache.sis.util.ArgumentChecks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Zarr encoder that writes variables and metadata to a Zarr dataset on the filesystem.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public final class ZarrEncoder extends Encoder {

    /**
     * The path of the folder where are stored the files of the Zarr format.
     */
    private final Path outputPath;

    /**
     * The root metadata of the Zarr dataset.
     */
    private final ZarrGroupMetadata metadata;

    /**
     * The default chunk shape to use if none is provided in {@link #buildVariable(String, Dimension[], Map, DataType, int[], int[], Object, Integer)}.
     */
    private final int[] chunkShape;

    /**
     * The default fill value to use
     */
    private final Object fillValue;

    /**
     * List of variables already written to the Zarr dataset.
     */
    private final List<VariableInfo> variables;

    /**
     * Creates a new decoder for the given file.
     *
     * @param  outputPath  the path of the folder where are stored the files of the Zarr format.
     * @param  geomlib    the library for geometric objects, or {@code null} for the default.
     * @param  listeners  where to send the warnings.
     * @throws IOException if an error occurred while reading the channel.
     * @throws DataStoreException if the content of the given channel is not a netCDF file.
     * @throws ArithmeticException if a variable is too large.
     */
    public ZarrEncoder(final Path outputPath, final int[] chunkShape, final Object fillValue, final GeometryLibrary geomlib,
                       final StoreListeners listeners) throws IOException, DataStoreException {
        super(geomlib, listeners);
        this.outputPath = outputPath;
        this.chunkShape = chunkShape;
        this.fillValue = fillValue;
        this.variables = new ArrayList<>();

        this.metadata = new ZarrGroupMetadata(outputPath.getFileName().toString(), outputPath, Map.of());
    }

    /**
     * Writes the Zarr metadata to the output path.
     * @throws IOException if an I/O error occurs.
     */
    private void writeMetadata() throws IOException {
        final ZarrMetadataWriter writer = new ZarrMetadataWriter();
        writer.writeZarrTree(metadata, outputPath);
    }

    /**
     * Writes the given variables to the Zarr dataset.
     *
     * @param variables the variables to write.
     * @throws IOException if an I/O error occurs.
     * @throws DataStoreException if a variable cannot be written.
     */
    @Override
    public void writeVariables(List<Variable> variables) throws IOException, DataStoreException {
        ArgumentChecks.ensureNonNull("variables", variables);

        List<String> dimensionsNames = getDimensionsNames(variables);

        for (Variable var : variables) {
            VariableInfo varInfo = (VariableInfo) var;
            String varName = varInfo.getName();

            boolean isDimensionVar = dimensionsNames.contains(varInfo.getName()); // Variable is a dimensions variable
            final int[] varShape = varInfo.metadata.shape();
            boolean isAuxiliaryVar = !isDimensionVar && varInfo.getNumDimensions() == 0
                    && (varShape == null || varShape.length == 0); // Variable is an auxiliary variable (no dimensions and no shape) (GeoZarr convention)

            // Resolve the target group for this variable from its path relative to the output root
            Path relativeGroupPath = outputPath.relativize(varInfo.metadata.path().getParent());
            ZarrGroupMetadata targetGroup = getOrCreateGroup(relativeGroupPath);

            // Check for existing variable in the target group:
            VariableInfo existingVar = null;
            ZarrNodeMetadata existingMeta = targetGroup.findChildNodeMetadata(varName);
            if (existingMeta != null) {
                // Try to find full VariableInfo by matching meta
                for (VariableInfo v : this.variables) {
                    if (v.metadata == existingMeta) {
                        existingVar = v;
                        break;
                    }
                }
            }

            // If another variable with the same name already exists, check if it's compatible (same metadata)
            // The goal is to avoid writing the same variable multiple times.
            // If not compatible, rename the variable by appending a unique suffix, e.g. varName_2, varName_3, etc.
            if (existingVar != null) {
                if (isCompatibleVariable(varInfo, existingVar)) {
                    // Variable with same metadata already written; skip re-writing it
                    continue;
                } else {
                    // Name clash! Find a new name, e.g. append a unique suffix
                    int suffix = 2;
                    String baseName = varName.replaceAll("\\d+$", "");
                    String newName;
                    do {
                        newName = baseName + suffix++;
                    } while (targetGroup.findChildNodeMetadata(newName) != null);
                    varInfo.setName(newName);
                    varName = newName;
                }
            }

            if (!isAuxiliaryVar) {
                varInfo.write();
            }

            this.variables.add(varInfo);
            targetGroup.addChildNodeMetadata(varInfo.metadata.name, varInfo.metadata);
        }

        this.writeMetadata();
    }

    /**
     * Walks the given relative path from the root group, creating intermediate {@link ZarrGroupMetadata}
     * nodes on the fly when they do not already exist.
     *
     * @param relativePath path relative to the output root (e.g. {@code "depth1/zone1"} or an empty path).
     * @return the group node at the end of the path, which may be the root group if the path is empty.
     * @throws IllegalStateException if a segment of the path already exists as an array node.
     */
    private ZarrGroupMetadata getOrCreateGroup(Path relativePath) {
        ZarrGroupMetadata current = this.metadata;
        for (Path segment : relativePath) {
            String segName = segment.toString();
            if (segName.isEmpty()) continue;
            ZarrNodeMetadata child = current.findChildNodeMetadata(segName);
            if (child == null) {
                ZarrGroupMetadata newGroup = new ZarrGroupMetadata(segName, current.path().resolve(segName), Map.of());
                current.addChildNodeMetadata(segName, newGroup);
                current = newGroup;
            } else if (child instanceof ZarrGroupMetadata) {
                current = (ZarrGroupMetadata) child;
            } else {
                throw new IllegalStateException("Expected a group at '" + segName + "' but found an array node.");
            }
        }
        return current;
    }

    private List<String> getDimensionsNames(List<Variable> variables) {
        List<String> dimensionsNames = new ArrayList<>();

        for (Variable variable : variables) {
            if (!(variable instanceof VariableInfo)) {
                throw new IllegalArgumentException("Variable is not an instance of VariableInfo: " + variable.getName());
            }
            VariableInfo variableInfo = (VariableInfo) variable;
            DimensionInfo[] dims = variableInfo.dimensions;
            if (dims == null || dims.length == 0) {
                continue; // No dimensions for this variable
            }
            dimensionsNames.addAll(Arrays.stream(dims).map(Dimension::getName)
                    .filter(n -> n != null) // Skip unnamed dimensions (null entries per Zarr v3 spec)
                    .collect(Collectors.toList()));
        }
        return dimensionsNames;
    }

    private boolean isCompatibleVariable(VariableInfo varA, VariableInfo varB) {
        // Check basic metadata, you can expand to check attributes, values, dtype, etc.
        if (!Arrays.equals(varA.metadata.shape(), varB.metadata.shape())) return false;
        if (!varA.getDataType().equals(varB.getDataType())) return false;
        // Check dimension names
        if (!Arrays.equals(varA.metadata.dimensionNames(), varB.metadata.dimensionNames())) return false;
        // Optionally check attribute map
        if (!varA.metadata.attributes.equals(varB.metadata.attributes)) return false;
        return true;
    }

    /**
     * Builds a dimension with the given name and length.
     * @param name the name of the dimension
     * @param length the length of the dimension
     * @return an array containing a single {@link DimensionInfo} object
     */
    @Override
    public Dimension buildDimension(final String name, int length) {
        return new DimensionInfo(name, length);
    }

    @Override
    public Variable buildVariable(String name, Dimension[] dimensions, Map<String, Object> attributes, DataType dataType,
                                  int[] shape, int[] chunkShape, Object data, Integer smIndex, Map<String, Object> configuration) throws DataStoreContentException {
        String[] dimensionNames = null;
        if (dimensions != null && dimensions.length > 0) {
            dimensionNames = Arrays.stream(dimensions)
                    .map(Dimension::getName)
                    .toArray(String[]::new);
        }

        //TODO : improve chunk shape handling
        if (shape.length == 1) {
            chunkShape = shape;
        }

        if (chunkShape == null) {
            if (shape.length == this.chunkShape.length) {
                chunkShape = this.chunkShape; // Use default chunk shape if not provided
            } else {
                chunkShape = shape; // Fallback to no chunking if shape length does not match default chunk shape length
            }
        }

        // Get separator from configuration if provided, otherwise use null (default separator used by Zarr is '/')
        Character separator = null;
        if (configuration != null && configuration.containsKey("separator")) {
            Object sepObj = configuration.get("separator");
            if (sepObj instanceof Character) {
                separator = (Character) sepObj;
            } else if (sepObj instanceof String && ((String) sepObj).length() == 1) {
                separator = ((String) sepObj).charAt(0);
            } else {
                throw new IllegalArgumentException("Invalid separator value in configuration: " + sepObj);
            }
        }

        // Get group path from configuration (e.g. "depth1" or "depth1/zone1"), defaults to root
        Path varBasePath = outputPath;
        if (configuration != null && configuration.containsKey("group")) {
            String groupPath = configuration.get("group").toString();
            if (!groupPath.isEmpty()) {
                varBasePath = outputPath.resolve(groupPath);
            }
        }

        ZarrArrayMetadata arrayMetadata = new ZarrArrayMetadata(name, varBasePath.resolve(name), attributes, dataType,
                shape, chunkShape, dimensionNames, fillValue, separator);

        // Explicit cast
        DimensionInfo[] dimInfos = null;
        if (dimensions != null && dimensions.length > 0) {
            dimInfos = Arrays.stream(dimensions)
                    .map(d -> (DimensionInfo) d)
                    .toArray(DimensionInfo[]::new);
        }

        return new VariableInfo(this, name, dimInfos, attributes, attributes.keySet(), dataType, arrayMetadata, data, smIndex);
    }

    /**
     * Returns a filename for formatting error message and for information purpose.
     * The filename does not contain path, but may contain file extension.
     *
     * @return a filename to include in warnings or error messages.
     */
    @Override
    public String getFilename() {
        return outputPath.getFileName().toString();
    }

    @Override
    public void close(DataStore lock) throws IOException {}
}