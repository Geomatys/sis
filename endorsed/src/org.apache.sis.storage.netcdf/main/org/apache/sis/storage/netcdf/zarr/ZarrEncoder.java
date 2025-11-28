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
            boolean isAuxiliaryVar = !isDimensionVar && varInfo.getNumDimensions() == 0; // Variable is an auxiliary variable (no dimensions) (GeoZarr convention)

            // Check for existing variable in group metadata:
            VariableInfo existingVar = null;
            ZarrNodeMetadata existingMeta = this.metadata.findChildNodeMetadata(varName);
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
                    } while (this.metadata.findChildNodeMetadata(newName) != null);
                    varInfo.setName(newName);
                    varName = newName;
                }
            }

            if (!isAuxiliaryVar) {
                varInfo.write();
            }

            this.variables.add(varInfo);
            this.metadata.addChildNodeMetadata(varInfo.metadata.name, varInfo.metadata);
        }

        this.writeMetadata();
    }

    private List<String> getDimensionsNames(List<Variable> variables) {
        List<String> dimensionsNames = new ArrayList<>();

        for (Variable variable : variables) {
            if (!(variable instanceof VariableInfo)) {
                throw new IllegalArgumentException("Variable is not an instance of VariableInfo: " + variable.getName());
            }
            VariableInfo variableInfo = (VariableInfo) variable;
            DimensionInfo[] dims = variableInfo.dimensions;
            dimensionsNames.addAll(Arrays.stream(dims).map(Dimension::getName).collect(Collectors.toList()));
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
    public Variable buildVariable(String name, Dimension[] dimensions, Map<String, Object> attributes, DataType dataType, int[] shape, int[] chunkShape, Object data, Integer smIndex) throws DataStoreContentException {
        String[] dimensionNames = Arrays.stream(dimensions)
                .map(Dimension::getName)
                .toArray(String[]::new);

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

        ZarrArrayMetadata arrayMetadata = new ZarrArrayMetadata(name, outputPath.resolve(name), attributes, dataType,
                shape, chunkShape, dimensionNames, fillValue);

        // Explicit cast
        DimensionInfo[] dimInfos = Arrays.stream(dimensions)
                .map(d -> (DimensionInfo) d)
                .toArray(DimensionInfo[]::new);

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