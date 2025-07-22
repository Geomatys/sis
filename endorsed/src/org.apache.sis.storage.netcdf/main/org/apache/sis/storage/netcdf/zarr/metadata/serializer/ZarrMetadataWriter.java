package org.apache.sis.storage.netcdf.zarr.metadata.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrArrayMetadata;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrGroupMetadata;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrNodeMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ZarrMetadataWriter {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Writes the Zarr metadata tree rooted at the given node to the given directory.
     * @param node The root node of the Zarr metadata tree.
     * @param dir The directory where the tree should be written.
     * @throws IOException if an error occurs during writing.
     */
    public void writeZarrTree(ZarrNodeMetadata node, Path dir) throws IOException {
        if (node instanceof ZarrGroupMetadata) {
            writeGroup((ZarrGroupMetadata) node, dir);
        } else if (node instanceof ZarrArrayMetadata) {
            writeArray((ZarrArrayMetadata) node, dir);
        } else {
            throw new IOException("Unknown node type: " + node.getClass());
        }
    }

    /**
     * Writes the metadata for a Zarr group to the specified directory.
     *
     * @param group the Zarr group metadata to write
     * @param dir the directory where the metadata should be written
     * @throws IOException if an error occurs while writing the metadata
     */
    private void writeGroup(ZarrGroupMetadata group, Path dir) throws IOException {
        Files.createDirectories(dir);
        Path zarrJson = dir.resolve("zarr.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(zarrJson.toFile(), group);

        for (Map.Entry<String, ZarrNodeMetadata> entry : group.getChildrenNodeMetadata().entrySet()) {
            String childName = entry.getKey();
            ZarrNodeMetadata childNode = entry.getValue();
            writeZarrTree(childNode, dir.resolve(childName));
        }
    }

    /**
     * Writes the metadata for a Zarr array to the specified directory.
     *
     * @param array the Zarr array metadata to write
     * @param dir the directory where the metadata should be written
     * @throws IOException if an error occurs while writing the metadata
     */
    private void writeArray(ZarrArrayMetadata array, Path dir) throws IOException {
        Files.createDirectories(dir);
        Path zarrJson = dir.resolve("zarr.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(zarrJson.toFile(), array);
    }
}
