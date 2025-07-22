package org.apache.sis.storage.netcdf.zarr.metadata.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrArrayMetadata;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrCodec;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrCodecInfo;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrGroupMetadata;
import org.apache.sis.storage.netcdf.zarr.metadata.ZarrNodeMetadata;
import org.apache.sis.system.Modules;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ZarrMetadataParser {

    /**
     * Logger for the Zarr metadata parser.
     */
    public static final Logger LOGGER = Logger.getLogger(ZarrMetadataParser.class.getName());

    /**
     * ObjectMapper instance for JSON parsing.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Reads the Zarr metadata tree starting from the given root directory.
     *
     * @param root the root directory of the Zarr dataset.
     * @return the root node of the Zarr metadata tree, which can be a {@link ZarrGroupMetadata} or {@link ZarrArrayMetadata}.
     *         The root node contains the metadata for the entire Zarr dataset.
     * @throws IOException if an error occurs while reading the metadata.
     */
    public ZarrNodeMetadata readZarrTree(Path root) throws IOException {
        return readZarrNode(root, root.getFileName().toString(), "/" + root.getFileName().toString());
    }

    /**
     * Reads the Zarr metadata for a specific node in the Zarr dataset.
     *
     * @param dir the directory containing the Zarr metadata files.
     * @param name the name of the node.
     * @param zarrPath the Zarr path of the node, used for serialization.
     * @return a {@link ZarrNodeMetadata} representing the node's metadata.
     * @throws IOException if an error occurs while reading the metadata.
     */
    private ZarrNodeMetadata readZarrNode(Path dir, String name, String zarrPath) throws IOException {

        // Check for the existence of the zarr.json file
        Path zarrJson = dir.resolve("zarr.json");
        if (!Files.exists(zarrJson)) {
            throw new IOException("Missing zarr.json in " + dir);
        }

        // Read the zarr.json file and check if it's a group or array
        JsonNode node = mapper.readTree(zarrJson.toFile());
        String nodeType = node.get("node_type").asText();

        if (nodeType.equalsIgnoreCase("group")) {
            ZarrGroupMetadata group = mapper.treeToValue(node, ZarrGroupMetadata.class);
            group.setName(name);
            group.setPath(dir);
            group.setZarrPath(zarrPath);

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path child : stream) {
                    if (Files.isDirectory(child)) {
                        if (Files.exists(child.resolve("zarr.json"))) {
                            String childName = child.getFileName().toString();
                            String subZarrPath = zarrPath + "/" + childName;
                            ZarrNodeMetadata childNode = readZarrNode(
                                    child,
                                    childName,
                                    subZarrPath
                            );
                            group.addChildNodeMetadata(childName, childNode);
                        }
                    }
                }
            }
            return group;

        } else if (nodeType.equalsIgnoreCase("array")) {
            ZarrArrayMetadata array = mapper.treeToValue(node, ZarrArrayMetadata.class);

            JsonNode codecsNode = node.get("codecs");
            array.codecs = parseCodecs(codecsNode);

            array.setName(name);
            array.setPath(dir);
            array.setZarrPath(zarrPath);
            array.setArrayDataPath(dir.resolve("c"));

            return array;
        } else {
            throw new IOException("Unknown node_type in Zarr metadata : " + nodeType);
        }
    }

    private List<ZarrCodecInfo> parseCodecs(JsonNode codecsArray) {
        List<ZarrCodecInfo> result = new ArrayList<>();
        if (codecsArray == null || !codecsArray.isArray()) return result;

        for (JsonNode codecNode : codecsArray) {
            String name = codecNode.path("name").asText();
            JsonNode configNode = codecNode.path("configuration");
            Map<String, Object> config = mapper.convertValue(configNode, Map.class);

            ZarrCodec codecEnum = null;
            try {
                codecEnum = ZarrCodec.valueOf(name.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOGGER.warning("Not supported Zarr codec: " + name + "");
            }
            result.add(new ZarrCodecInfo(codecEnum, name, config));
        }
        return result;
    }
}
