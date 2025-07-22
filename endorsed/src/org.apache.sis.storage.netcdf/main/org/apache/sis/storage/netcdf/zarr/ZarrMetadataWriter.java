package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Serializes (writes) a Zarr tree structure to disk by writing zarr.json metadata files for groups and arrays.
 * Call {@link #writeZarrTree} with the root metadata and target root directory.
 *
 * @author Quentin Bialota (Geomatys)
 */
final class ZarrMetadataWriter {

    /**
     * Logger for the Zarr metadata writer.
     */
    public static final Logger LOGGER = Logger.getLogger(ZarrMetadataWriter.class.getName());

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * Writes the Zarr metadata tree rooted at the given node to the given directory.
     * @param node The root node of the Zarr metadata tree.
     * @param dir The directory where the tree should be written.
     * @throws IOException if an error occurs during writing.
     */
    public void writeZarrTree(ZarrNodeMetadata node, Path dir) throws IOException {
        // Create directory if necessary
        if (!Files.exists(dir)) Files.createDirectories(dir);

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
        Path zarrJson = dir.resolve("zarr.json");
        if (zarrJson.toFile().exists()) {
            LOGGER.warning("Existing zarr.json (group) at " + zarrJson + ", skipping write.");
        } else {
            // Write groupe metadata file
            mapper.writerWithDefaultPrettyPrinter().writeValue(zarrJson.toFile(), group);
        }

        // Recursively write children
        for (Map.Entry<String, ZarrNodeMetadata> entry : group.getChildrenNodeMetadata().entrySet()) {
            String childName = entry.getKey();
            ZarrNodeMetadata childNode = entry.getValue();
            writeZarrTree(childNode, dir.resolve(childName));
        }
    }

    /**
     * Writes the metadata for a Zarr array to the specified directory.
     *+
     * @param array the Zarr array metadata to write
     * @param dir the directory where the metadata should be written
     * @throws IOException if an error occurs while writing the metadata
     */
    private void writeArray(ZarrArrayMetadata array, Path dir) throws IOException {
        Path zarrJson = dir.resolve("zarr.json");
        if (zarrJson.toFile().exists()) {
            LOGGER.warning("Existing zarr.json (array) at " + zarrJson + ", skipping write.");
            return;
        }

        ObjectNode root = mapper.valueToTree(array);
        // Replace or inject the "codecs" field with your correct codecs info:
        ArrayNode codecsJson = codecsToJson(array.codecs(), mapper);
        root.set("codecs", codecsJson);

        mapper.writerWithDefaultPrettyPrinter().writeValue(zarrJson.toFile(), root);
    }

    /**
     * Converts a list of Zarr codecs to a JSON array representation.
     * @param codecs the list of Zarr codecs to convert
     * @param mapper the ObjectMapper used for serialization
     * @return an ArrayNode containing the JSON representation of the codecs
     */
    private ArrayNode codecsToJson(List<AbstractZarrCodec> codecs, ObjectMapper mapper) {
        ArrayNode codecsArray = mapper.createArrayNode();
        for (AbstractZarrCodec codec : codecs) {
            ObjectNode codecNode = mapper.createObjectNode();
            codecNode.put("name", codec.codec().name().toLowerCase());
            Map<String, Object> conf = codec.configuration;
            if (conf != null) {
                codecNode.set("configuration", mapper.valueToTree(conf));
            } else {
                codecNode.putNull("configuration");
            }
            codecsArray.add(codecNode);
        }
        return codecsArray;
    }
}
