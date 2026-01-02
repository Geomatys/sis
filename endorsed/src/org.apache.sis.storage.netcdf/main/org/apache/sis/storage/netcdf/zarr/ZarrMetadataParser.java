package org.apache.sis.storage.netcdf.zarr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Parses Zarr metadata from the filesystem and constructs a tree structure of
 * Zarr nodes.
 * This class reads the `zarr.json` files in the Zarr dataset directories and
 * builds the metadata tree structure.
 *
 * @author Quentin Bialota (Geomatys)
 */
final class ZarrMetadataParser {

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
     * @return the root node of the Zarr metadata tree, which can be a
     *         {@link ZarrGroupMetadata} or {@link ZarrArrayMetadata}.
     *         The root node contains the metadata for the entire Zarr dataset.
     * @throws IOException if an error occurs while reading the metadata.
     */
    public ZarrNodeMetadata readZarrTree(Path root) throws IOException {
        return readZarrNode(root, root.getFileName().toString(), "/" + root.getFileName());
    }

    /**
     * Reads the Zarr metadata for a specific node in the Zarr dataset.
     *
     * @param dir      the directory containing the Zarr metadata files.
     * @param name     the name of the node.
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
        JsonNode node;
        try (InputStream in = Files.newInputStream(zarrJson)) {
            node = mapper.readTree(in);
        } catch (IOException e) {
            throw new IOException("Error reading zarr.json in " + dir, e);
        }

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
                                    subZarrPath);
                            group.addChildNodeMetadata(childName, childNode);
                        }
                    }
                }
            }

            // Check for multiscales attribute in the group attributes
            if (node.has("attributes")) {
                JsonNode attributesNode = node.get("attributes");
                if (attributesNode.has("multiscales")) {
                    JsonNode multiscalesNode = attributesNode.get("multiscales");
                    try {
                        if (multiscalesNode.isArray()) {
                            List<ZarrMultiscale> multiscales = mapper.readerForListOf(ZarrMultiscale.class)
                                    .readValue(multiscalesNode);
                            group.setMultiscales(multiscales);
                        } else {
                            ZarrMultiscale multiscale = mapper.treeToValue(multiscalesNode, ZarrMultiscale.class);
                            group.setMultiscales(List.of(multiscale));
                        }
                    } catch (IOException e) {
                        LOGGER.warning(
                                "Failed to parse multiscales metadata for group " + name + ": " + e.getMessage());
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

    /**
     * Parses the codecs defined in the Zarr metadata.
     *
     * @param codecsArray the JSON array containing codec definitions.
     * @return a list of {@link AbstractZarrCodec} instances representing the
     *         codecs.
     */
    private List<AbstractZarrCodec> parseCodecs(JsonNode codecsArray) {
        List<AbstractZarrCodec> result = new ArrayList<>();
        if (codecsArray == null || !codecsArray.isArray())
            return result;

        for (JsonNode codecNode : codecsArray) {
            String name = codecNode.path("name").asText();
            JsonNode configNode = codecNode.path("configuration");
            Map<String, Object> config = mapper.convertValue(configNode, Map.class);

            ZarrCodec codecEnum = null;
            Class<?> codecClass = null;
            try {
                codecEnum = ZarrCodec.valueOf(name.toUpperCase());
                codecClass = codecEnum.codecClass();
            } catch (IllegalArgumentException e) {
                LOGGER.warning("Not supported Zarr codec: " + name);
            }
            if (codecEnum == null || codecClass == null) {
                LOGGER.warning("Skipping unsupported Zarr codec: " + name);
                continue;
            }

            // TODO: Add support for other codecs if needed
            switch (codecEnum) {
                case BYTES:
                    result.add(new BytesCodec(config));
                    break;
                case VLEN_UTF8:
                    result.add(new VlenUtf8Codec(config));
                    break;
                case ZSTD:
                    result.add(new ZstdCodec(config));
                    break;
            }
        }
        return result;
    }
}
