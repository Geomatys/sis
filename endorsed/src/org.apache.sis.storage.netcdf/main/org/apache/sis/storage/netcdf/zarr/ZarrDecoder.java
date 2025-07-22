package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.math.Vector;
import org.apache.sis.measure.Units;
import org.apache.sis.pending.jdk.JDK19;
import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStore;
import org.apache.sis.storage.DataStoreContentException;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.base.MetadataBuilder;
import org.apache.sis.storage.event.StoreListeners;
import org.apache.sis.storage.netcdf.base.Convention;
import org.apache.sis.storage.netcdf.base.Decoder;
import org.apache.sis.storage.netcdf.base.Dimension;
import org.apache.sis.storage.netcdf.base.Grid;
import org.apache.sis.storage.netcdf.base.NamedElement;
import org.apache.sis.storage.netcdf.base.Node;
import org.apache.sis.storage.netcdf.base.Variable;
import org.apache.sis.temporal.LenientDateFormat;
import org.apache.sis.temporal.TemporalDate;
import org.apache.sis.util.collection.TreeTable;
import org.apache.sis.util.internal.shared.CollectionsExt;
import org.opengis.parameter.InvalidParameterCardinalityException;

import javax.measure.IncommensurableException;
import javax.measure.UnitConverter;
import javax.measure.format.MeasurementParseException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.sis.util.internal.shared.Constants;

/**
 * Provides Zarr decoding services as a standalone library.
 * The javadoc in this class uses the "file" word for the source of data, but
 * this implementation actually works with arbitrary {@link ReadableByteChannel}.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public final class ZarrDecoder extends Decoder {
    /**
     * The {@link ReadableByteChannel} together with a {@link ByteBuffer} for reading the data.
     */
    private final Path inputPath;

    /**
     * The metadata of the Zarr node.
     * This is initialized in the constructor by reading the metadata from the Zarr root path.
     */
    private final ZarrNodeMetadata metadata;

    /**
     * All dimensions in the zarr structure.
     */
    private Map<String, DimensionInfo> dimensionMap = new HashMap<>();

    /**
     * The attributes found in the zarr tree structure.
     * Values in this map give directly the attribute value (there is no {@code Attribute} object).
     * Values are {@link String}, wrappers such as {@link Double}, or {@link Vector} objects.
     *
     * @see #findAttribute(String)
     */
    private final Map<String, List<AttributeEntry>> attributeMap;

    /**
     * Names of attributes. This is {@code attributeMap.keySet()} unless some attributes have a name
     * containing upper case letters. In such case a separated set is created for avoiding duplicated
     * names (the name with upper case letters + the name in all lower case letters).
     *
     * @see #getAttributeNames()
     */
    private final Set<String> attributeNames;

    /**
     * The variables found in the zarr structure.
     *
     * @see #getVariables()
     */
    final VariableInfo[] variables;

    /**
     * Contains all {@link #variables}, but as a map for faster lookup by name. The same {@link VariableInfo}
     * instance may be repeated in two entries if the original variable name contains upper case letters.
     * In such case, the value is repeated and associated to a key in all lower case key letters.
     *
     * @see #findVariable(String)
     */
    private final Map<String, VariableInfo> variableMap;

    /**
     * The Zarr groups found in the zarr structure.
     */
    private Map<String, ZarrGroupMetadata> groupMap = new HashMap<>();

    /** The grid geometries, created when first needed.
     *
     * @see #getGridCandidates()
     */
    private transient Grid[] gridGeometries;

    /**
     * Creates a new decoder for the given file.
     *
     * @param  inputPath  the path of the folder where are stored the files of the Zarr format.
     * @param  geomlib    the library for geometric objects, or {@code null} for the default.
     * @param  listeners  where to send the warnings.
     * @throws IOException if an error occurred while reading the channel.
     * @throws DataStoreException if the content of the given channel is not a netCDF file.
     * @throws ArithmeticException if a variable is too large.
     */
    public ZarrDecoder(final Path inputPath, final GeometryLibrary geomlib,
                       final StoreListeners listeners) throws IOException, DataStoreException {
        super(geomlib, listeners);

        this.inputPath = inputPath;

        // Initialize the input channel.
        this.metadata = decodeZarrMetadata(inputPath);
        if (this.metadata == null) {
            throw new DataStoreException("Zarr metadata could not be read from " + inputPath);
        }

        DimensionInfo[] dimensions = null;
        List<AttributeEntry> attributes = List.of();

        dimensions = readDimensions(this.metadata);
        attributes = readAttributes(this.metadata, false);
        this.variables = readVariables(this.metadata, dimensions);

        attributeMap = toCaseInsensitiveNameMap(attributes.toArray(new AttributeEntry[0]));
        attributeNames = attributeNames(attributes, attributeMap);

        variableMap = toCaseInsensitiveNameMap(this.variables);
    }

    /**
     * Creates a (<var>name</var>, <var>element</var>) mapping for the given array of elements.
     * If the name of an element is not all lower cases, then this method also adds an entry for the
     * lower cases version of that name in order to allow case-insensitive searches.
     *
     * <p>Code searching in the returned map shall ask for the original (non lower-case) name
     * <strong>before</strong> to ask for the lower-cases version of that name.</p>
     *
     * @param  <E>       the type of elements.
     * @param  elements  the elements to store in the map, or {@code null} if none.
     * @return a (<var>name</var>, <var>element</var>) mapping with lower cases entries where possible.
     * @throws InvalidParameterCardinalityException if the same name is used for more than one element.
     */
    private static <E extends NamedElement> Map<String,E> toCaseInsensitiveNameMap(final E[] elements) {
        return CollectionsExt.toCaseInsensitiveNameMap(new AbstractList<Map.Entry<String,E>>() {
            @Override
            public int size() {
                return elements.length;
            }

            @Override
            public Map.Entry<String,E> get(final int index) {
                final E e = elements[index];
                return new AbstractMap.SimpleImmutableEntry<>(e.getName(), e);
            }
        }, Decoder.DATA_LOCALE);
    }

    /**
     * Creates a (<var>name</var>, <var>list of entries</var>) mapping for the given array of attribute entries.
     * This method is used for storing attributes in a way that allows case-insensitive searches.
     *
     * @param entries the attribute entries to store in the map, or {@code null} if none.
     * @return a (<var>name</var>, <var>list of entries</var>) mapping with lower cases entries where possible.
     */
    private static Map<String, List<AttributeEntry>> toCaseInsensitiveNameMap(final AttributeEntry[] entries) {
        if (entries == null || entries.length == 0) {
            return Collections.emptyMap();
        }

        Map<String, List<AttributeEntry>> map = new LinkedHashMap<>(entries.length);
        for (AttributeEntry entry : entries) {
            String name = entry.name;
            String lowerName = name.toLowerCase(Decoder.DATA_LOCALE);
            map.computeIfAbsent(lowerName, k -> new ArrayList<>()).add(entry);
        }
        return map;
    }

    /**
     * Checks if the given path is a Zarr dataset.
     *
     * @param path the path to the Zarr dataset root.
     * @return {@code true} if the path is a Zarr dataset, {@code false} otherwise.
     * @throws IOException if an error occurs while reading the metadata.
     */
    public static boolean isZarr(final Path path) throws IOException {
        try {
            ZarrNodeMetadata metadata = decodeZarrMetadata(path);
            if (metadata != null) {
                return true;
            }
        } catch (IOException | DataStoreException e) {
            // If we cannot read the metadata, it is not a Zarr dataset.
            return false;
        }
        return false;
    }

    /**
     * Returns the metadata of the Zarr node.
     * This method is used to access the metadata after the decoder has been created.
     *
     * @param zarrRootPath the path to the root of the Zarr dataset.
     * @return the metadata of the Zarr node.
     */
    private static ZarrNodeMetadata decodeZarrMetadata(final Path zarrRootPath) throws IOException, DataStoreException {
        // 1. Path is directory?
        if (!Files.isDirectory(zarrRootPath)) {
            throw new DataStoreException("Specified path is not a directory: " + zarrRootPath + ". Zarr dataset root must be a directory.");
        }

        // 2. Directory is not empty?
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(zarrRootPath)) {
            if (!stream.iterator().hasNext()) {
                throw new DataStoreContentException("Zarr root directory specified is empty: " + zarrRootPath + ". No data to read." );
            }
        }

        // 3. zarr.json must exist
        Path zarrJson = zarrRootPath.resolve("zarr.json");
        if (!Files.exists(zarrJson)) {
            throw new DataStoreContentException("Missing zarr.json in " + zarrRootPath);
        }

        ZarrMetadataParser parser = new ZarrMetadataParser();
        // Read the Zarr metadata from the zarr tree.
        return parser.readZarrTree(zarrRootPath);
    }

    // --------------------------------------------------------------------------------------------
    //  DIMENSION READING / MANAGEMENT
    // --------------------------------------------------------------------------------------------

    /**
     * Reads dimensions from the Zarr metadata object. The record structure is:
     *
     * <ul>
     *   <li>The dimension name</li>
     *   <li>The dimension length</li>
     *   <li>The dimension array paths (list of arrays that use the dimension)</li>
     * </ul>
     *
     * @param  metadata the root node of the Zarr metadata tree.
     * @return the dimensions in the order they are found in the Zarr metadata tree.
     */
    private DimensionInfo[] readDimensions(final ZarrNodeMetadata metadata) throws IOException, DataStoreException {
        final List<DimensionInfo> dimensions = new ArrayList<>();

        extractDimensionsRecursively(metadata, dimensions, "");

        DimensionInfo[] array = dimensions.toArray(new DimensionInfo[0]);
        this.dimensionMap = toCaseInsensitiveNameMap(array);
        return array;
    }
    /**
     * The letters used for dimension names in Zarr.
     * This is a static array of characters that can be used to generate dimension names.
     * The first 26 letters are used for the first 26 dimensions, and then it continues with 'a', 'b', etc.
     */
    private static final char[] DIM_LETTERS = {'x', 'y', 'z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w'};

    /**
     * Generate a unique dimension name based on the index and existing dimensions.
     */
    private String generateDimensionName(int dimIndex, int size, List<DimensionInfo> dimensions) {
        String baseName = String.valueOf(DIM_LETTERS[dimIndex % DIM_LETTERS.length]);
        String name = baseName;
        int suffix = 2;
        // Check if the name is already used for a different size
        while (dimensionNameUsed(name, size, dimensions)) {
            name = baseName + suffix;
            suffix++;
        }
        return name;
    }

    /**
     * Checks if a dimension name is already used with a different size.
     * @param name the name of the dimension to check.
     * @param size the size of the dimension to check.
     * @param dimensions the list of existing dimensions to check against.
     * @return {@code true} if the name is used with a different size, {@code false} otherwise.
     */
    private boolean dimensionNameUsed(String name, int size, List<DimensionInfo> dimensions) {
        for (DimensionInfo d : dimensions) {
            if (d.name.equals(name) && d.length != size) {
                return true;
            }
        }
        return false;
    }

    /**
     * Recursively extracts dimensions from the Zarr metadata tree.
     * @param node the current Zarr node metadata to process.
     * @param dimensions the list to which found dimensions will be added.
     * @param parentPath the path of the parent node, used to build the full path of the current node.
     * @throws IOException if an I/O error occurs while reading the metadata.
     * @throws DataStoreException if a logical error occurs while processing the metadata.
     */
    private void extractDimensionsRecursively(final ZarrNodeMetadata node, final List<DimensionInfo> dimensions, String parentPath) throws IOException, DataStoreException {
        String currentPath = parentPath.isEmpty() ? "/" + node.name() : parentPath + "/" + node.name();

        if (node instanceof ZarrArrayMetadata) {
            ZarrArrayMetadata array = (ZarrArrayMetadata) node;
            int[] shape = array.shape();
            String[] dimNames = array.dimensionNames();

            for (int i = 0; i < shape.length; i++) {
                String name;
                if (dimNames != null && dimNames.length > i && dimNames[i] != null) {
                    name = dimNames[i];
                } else {
                    name = generateDimensionName(i, shape[i], dimensions);
                }

                // Search for existing DimensionInfo with same name and size
                DimensionInfo info = null;
                for (DimensionInfo d : dimensions) {
                    if (d.name.equals(name) && d.length == shape[i]) {
                        info = d;
                        break;
                    }
                }
                if (info == null) {
                    info = new DimensionInfo(name, shape[i]);
                    dimensions.add(info);
                }
                info.addArrayPath(currentPath, array.dimensionNames());
            }

        } else if (node instanceof ZarrGroupMetadata) {
            ZarrGroupMetadata group = (ZarrGroupMetadata) node;
            for (ZarrNodeMetadata child : group.getChildrenNodeMetadata().values()) {
                extractDimensionsRecursively(child, dimensions, currentPath);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  ATTRIBUTES READING / MANAGEMENT
    // --------------------------------------------------------------------------------------------

    /**
     * Represents an entry in the attributes map.
     */
    static class AttributeEntry extends NamedElement {
        /**
         * The path of the Zarr node where this attribute is defined.
         * Format: "/group/subgroup/array" or "/array" for root arrays.
         */
        public final String path;

        /**
         * The name of the attribute.
         */
        public final String name;

        /**
         * The value of the attribute.
         */
        public final Object value;

        /**
         * Creates a new attribute entry.
         *
         * @param path  the path of the Zarr node where this attribute is defined.
         * @param name  the name of the attribute.
         * @param value the value of the attribute.
         */
        public AttributeEntry(String path, String name, Object value) {
            this.path = path; this.name = name; this.value = value;
        }

        /**
         * Converts this attribute entry to a map entry.
         * @return a map entry with the attribute name as key and the value as value.
         */
        public Map.Entry<String, Object> toMapEntry() {
            return new AbstractMap.SimpleEntry<>(name, value);
        }

        @Override
        public String getName() {
             return name;
        }
    }

    /**
     * Reads attributes from the Zarr metadata object.
     *
     * @param metadata the root node of the Zarr metadata tree.
     * @param onlyGroupAttributes if {@code true}, only attributes of groups will be extracted.
     * @return a list of attributes found in the Zarr metadata tree.
     */
    private List<AttributeEntry> readAttributes(final ZarrNodeMetadata metadata, boolean onlyGroupAttributes) {
        final List<AttributeEntry> attributes = new ArrayList<>();
        extractAttributesRecursively(metadata, attributes, onlyGroupAttributes);
        return attributes;
    }

    /**
     * Recursively extracts attributes from the Zarr metadata tree.
     *
     * @param node the current Zarr node metadata to process.
     * @param attributes the list to which found attributes will be added.
     * @param onlyGroupAttributes if {@code true}, only attributes of groups will be extracted.
     */
    private void extractAttributesRecursively(final ZarrNodeMetadata node, final List<AttributeEntry> attributes, boolean onlyGroupAttributes) {
        //Only store attributes for groups, this will exclude array attributes (also named variables attributes).
        if (onlyGroupAttributes && node instanceof ZarrArrayMetadata) {
            return;
        }

        if (node.attributes() != null && !node.attributes().isEmpty()) {
            for (Map.Entry<String, Object> entry : node.attributes().entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) value = ((String)value).trim();
                attributes.add(new AttributeEntry(node.zarrPath(), entry.getKey(), value));
            }
        }

        if (node instanceof ZarrGroupMetadata) {
            ZarrGroupMetadata group = (ZarrGroupMetadata) node;
            for (ZarrNodeMetadata child : group.getChildrenNodeMetadata().values()) {
                extractAttributesRecursively(child, attributes, onlyGroupAttributes);
            }
        }
    }

    /**
     * Returns the keys of {@code attributeMap} without the duplicated values caused by the change of name case.
     * For example if an attribute {@code "Foo"} exists and a {@code "foo"} key has been generated for enabling
     * case-insensitive search, only the {@code "Foo"} name is added in the returned set.
     *
     * @param  attributes    the attributes returned by {@link #readAttributes(ZarrNodeMetadata, boolean)}.
     * @param  attributeMap  the map created by {@link CollectionsExt#toCaseInsensitiveNameMap(Collection, Locale)}.
     * @return {@code attributes.keySet()} without duplicated keys.
     */
    private static Set<String> attributeNames(final List<AttributeEntry> attributes, final Map<String,?> attributeMap) {
        if (attributes.size() >= attributeMap.size()) {
            return Collections.unmodifiableSet(attributeMap.keySet());
        }
        final Set<String> attributeNames = JDK19.newLinkedHashSet(attributes.size());
        attributes.forEach((e) -> attributeNames.add(e.getName()));
        return attributeNames;
    }

    private static Set<String> attributeNamesWithMapEntries(final List<Map.Entry<String, Object>> attributes, final Map<String,?> attributeMap) {
        if (attributes.size() >= attributeMap.size()) {
            return Collections.unmodifiableSet(attributeMap.keySet());
        }
        final Set<String> attributeNames = JDK19.newLinkedHashSet(attributes.size());
        attributes.forEach((e) -> attributeNames.add(e.getKey()));
        return attributeNames;
    }

    /**
     * Returns the zarr attribute of the given name, or {@code null} if none. This method is invoked
     * for every global attributes to be read by this class (but not {@linkplain VariableInfo variable}
     * attributes), thus providing a single point where we can filter the attributes to be read.
     * The {@code name} argument is typically (but is not restricted to) one of the constants
     * defined in the {@link org.apache.sis.storage.netcdf.AttributeNames} class.
     *
     * @param  name  the name of the attribute to search, or {@code null}.
     * @return the attribute value, or {@code null} if none.
     *
     * @see #getAttributeNames()
     */
    @SuppressWarnings("StringEquality")
    private AttributeEntry findAttribute(final String name) {
        if (name == null) {
            return null;
        }
        int index = 0;
        String mappedName;
        final Convention convention = convention();
        while ((mappedName = convention.mapAttributeName(name, index++)) != null) {
            AttributeEntry value = (attributeMap.get(mappedName) != null) ? attributeMap.get(mappedName).get(0) : null;
            if (value != null) return value;
            /*
             * If no value were found for the given name, tries the following alternatives:
             *
             *   - Same name but in lower cases.
             *   - Alternative name specific to the non-standard convention used by current file.
             *   - Same alternative name but in lower cases.
             *
             * Identity comparisons performed between String instances below are okay since they
             * are only optimizations for skipping calls to Map.get(Object) in common cases.
             */
            final String lowerCase = mappedName.toLowerCase(DATA_LOCALE);
            if (lowerCase != mappedName) {
                value = (attributeMap.get(lowerCase) != null) ? attributeMap.get(lowerCase).get(0) : null;
                if (value != null) return value;
            }
        }
        return null;
    }

    // --------------------------------------------------------------------------------------------
    //  VARIABLE READING / MANAGEMENT
    // --------------------------------------------------------------------------------------------

    private VariableInfo[] readVariables(final ZarrNodeMetadata metadata, final DimensionInfo[] allDimensions) throws IOException, DataStoreException {
        final List<VariableInfo> variables = new ArrayList<>();

        // If Array, read its metadata and create a VariableInfo object
        if (metadata instanceof ZarrArrayMetadata) {
            ZarrArrayMetadata array = (ZarrArrayMetadata) metadata;
            String name = array.name();

            // One dimension per axis
            DimensionInfo[] dimensions = new DimensionInfo[array.shape().length];

            List<Map.Entry<String, Object>> attributes = readAttributes(metadata, false).stream().map(AttributeEntry::toMapEntry).collect(Collectors.toList());
            final Map<String,Object> attributeMap = CollectionsExt.toCaseInsensitiveNameMap(attributes, Decoder.DATA_LOCALE);

            int i = 0;
            for (DimensionInfo d : allDimensions) {
                if (d.isDimensionUsedInArray(metadata.zarrPath())) {
                    dimensions[i] = d;
                    i++;
                }
            }

            variables.add(new VariableInfo(this, name, dimensions, attributeMap, attributeNamesWithMapEntries(attributes, attributeMap), array.dataType(), array));


        // If Group, read its children variables recursively
        } else if (metadata instanceof ZarrGroupMetadata) {
            ZarrGroupMetadata group = (ZarrGroupMetadata) metadata;
            for (ZarrNodeMetadata child : group.getChildrenNodeMetadata().values()) {
                variables.addAll(Arrays.asList(readVariables(child, allDimensions)));
            }
        }

        // Sort variables by their path in the Zarr structure for ensuring a deterministic order.
        variables.sort(Comparator.comparing(v -> v.metadata.zarrPath()));

        return variables.toArray(new VariableInfo[0]);
    }

    // --------------------------------------------------------------------------------------------
    //  GROUPS / SEARCH PATH MANAGEMENT
    // --------------------------------------------------------------------------------------------

    /**
     * Finds a Zarr group by its path or by name.
     * If the name contains '/', it is treated as a path (absolute or relative).
     * If the name does not contain '/', the first group with that name is returned (searched recursively).
     *
     * @param name The group name or path.
     * @return The found ZarrGroupMetadata, or null if not found.
     */
    private ZarrGroupMetadata findGroup(final String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }

        // Path-based search
        if (name.contains("/")) {
            // Split and skip empty segments (to support leading '/')
            String[] parts = Arrays.stream(name.split("/"))
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);

            ZarrNodeMetadata current = metadata;

            if (parts.length == 1 && parts[0].equals(metadata.name())) {
                return (current instanceof ZarrGroupMetadata) ? (ZarrGroupMetadata) current : null;
            }

            // Part[0] is the root
            for (int i = 1; i < parts.length; i++) {
                if (!(current instanceof ZarrGroupMetadata)) {
                    return null;
                }
                ZarrGroupMetadata group = (ZarrGroupMetadata) current;

                current = group.getChildrenNodeMetadata().get(parts[i]);
                if (current == null) {
                    return null;
                }
            }
            return (current instanceof ZarrGroupMetadata) ? (ZarrGroupMetadata) current : null;
        }

        // Name-based search: recursive search for the first group with the given name
        return findGroupByNameRecursive(metadata, name);
    }

    private ZarrGroupMetadata findGroupByNameRecursive(ZarrNodeMetadata node, final String name) {
        if (node instanceof ZarrGroupMetadata) {
            ZarrGroupMetadata group = (ZarrGroupMetadata) node;
            if (name.equals(group.name())) {
                return group;
            }
            for (ZarrNodeMetadata child : group.getChildrenNodeMetadata().values()) {
                ZarrGroupMetadata found = findGroupByNameRecursive(child, name);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    // --------------------------------------------------------------------------------------------
    //  Decoder API begins below this point. Above code was specific to parsing of zarr header.
    // --------------------------------------------------------------------------------------------

    @Override
    public void addAttributesTo(TreeTable.Node root) {

    }

    /**
     * Returns a filename for formatting error message and for information purpose.
     * The filename does not contain path, but may contain file extension.
     *
     * @return a filename to include in warnings or error messages.
     */
    @Override
    public final String getFilename() {
        return inputPath.getFileName().toString();
    }

    /**
     * Sets an identification of the file format. This method uses a reference to a database entry
     * known to {@link org.apache.sis.metadata.sql.MetadataSource#lookup(Class, String)}.
     */
    @Override
    public void addFormatDescription(MetadataBuilder builder) {
        builder.setPredefinedFormat(Constants.ZARR, null, true);
        builder.addFormatReaderSIS(Constants.ZARR);
    }

    /**
     * Sets the search path for groups in the Zarr structure.
     * This method allows to specify a list of group names in preference order.
     * NOTE :
     * - If a a group name contains '/' (slash), it is considered as a path to a group
     *   (e.g. "/group/subgroup", will search for "subgroup" in the "group" group).
     * - If a group name does not contain '/', it is considered as a group name, and will be searched in all the tree until a match is found.
     * - If a group name is not found in the Zarr structure, it is ignored.
     * - If a group name is {@code null}, it is ignored.
     *
     * @param groupNames  the name of the group where to search, in preference order.
     *
     */
    @Override
    public void setSearchPath(String... groupNames) {
        this.groupMap = new HashMap<>();

        for (final String name : groupNames) {
            if (name != null) {
                final ZarrGroupMetadata group = findGroup(name);
                if (group == null) {
                    continue;
                }
                this.groupMap.put(name, group);
            }
        }
    }

    @Override
    public String[] getSearchPath()
    {
        return this.groupMap.keySet().toArray(new String[this.groupMap.size()]);
    }

    /**
     * Returns the names of all global attributes found in the file.
     * The returned set is unmodifiable.
     *
     * @return names of all global attributes in the file.
     */
    @Override
    public Collection<String> getAttributeNames() {
        return Collections.unmodifiableSet(attributeNames);
    }

    /**
     * Returns the value for the attribute of the given name, or {@code null} if none.
     *
     * @param  name  the name of the attribute to search, or {@code null}.
     * @return the attribute value, or {@code null} if none or empty or if the given name was null.
     */
    @Override
    public String stringValue(final String name) {
        final AttributeEntry attribute = findAttribute(name);
        if (attribute == null) {
            return null;
        }
        return (attribute.value != null) ? attribute.value.toString() : null;
    }

    /**
     * Returns the value of the attribute of the given name as a number, or {@code null} if none.
     * If there is more than one numeric value, only the first one is returned.
     *
     * @return {@inheritDoc}
     */
    @Override
    public Number numericValue(final String name) {
        final AttributeEntry attribute = findAttribute(name);
        if (attribute == null) {
            return null;
        }
        if (attribute.value instanceof Number) {
            return (Number) attribute.value;
        } else if (attribute.value instanceof String) {
            return parseNumber(name, (String) attribute.value);
        } else if (attribute.value instanceof Vector) {
            return ((Vector) attribute.value).get(0);
        } else {
            return null;
        }
    }

    /**
     * Returns the value of the attribute of the given name as a date, or {@code null} if none.
     * If there is more than one numeric value, only the first one is returned.
     *
     * @return {@inheritDoc}
     */
    @Override
    public Temporal dateValue(final String name) {
        final AttributeEntry attribute = findAttribute(name);
        if (attribute == null) {
            return null;
        }
        if (attribute.value instanceof CharSequence) try {
            return LenientDateFormat.parseBest((CharSequence) attribute.value);
        } catch (RuntimeException e) {
            listeners.warning(e);
        }
        return null;
    }

    /**
     * Converts the given numerical values to date, using the information provided in the given unit symbol.
     * The unit symbol is typically a string like <q>days since 1970-01-01T00:00:00Z</q>.
     *
     * @param  values  the values to convert. May contain {@code null} elements.
     * @return the converted values. May contain {@code null} elements.
     */
    @Override
    public Temporal[] numberToDate(final String symbol, final Number... values) {
        final var dates = new Instant[values.length];
        final Matcher parts = Variable.TIME_UNIT_PATTERN.matcher(symbol);
        if (parts.matches()) try {
            final UnitConverter converter = Units.valueOf(parts.group(1)).getConverterToAny(Units.SECOND);
            final Instant epoch = LenientDateFormat.parseInstantUTC(parts.group(2));
            for (int i=0; i<values.length; i++) {
                final Number value = values[i];
                if (value != null) {
                    dates[i] = TemporalDate.addSeconds(epoch, converter.convert(value.doubleValue()));
                }
            }
        } catch (IncommensurableException | MeasurementParseException | DateTimeException | ArithmeticException e) {
            listeners.warning(e);
        }
        return dates;
    }

    /**
     * Returns all variables found in the zarr file.
     * This method returns a direct reference to an internal array - do not modify.
     *
     * @return {@inheritDoc}
     */
    @Override
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public Variable[] getVariables() {
        return variables;
    }

    /**
     * Adds to the given set all variables of the given names. This operation is performed when the set of axes is
     * specified by a {@code "coordinates"} attribute associated to a data variable, or by customized conventions
     * specified by {@link org.apache.sis.storage.netcdf.base.Convention#namesOfAxisVariables(Variable)}.
     *
     * @param  names       names of variables containing axis data, or {@code null} if none.
     * @param  axes        where to add named variables.
     * @param  dimensions  where to report all dimensions used by added axes.
     * @return whether {@code names} was non-null and non-empty.
     */
    private boolean listAxes(final CharSequence[] names, final Set<VariableInfo> axes, final Set<DimensionInfo> dimensions) {
        if (names == null || names.length == 0) {
            return false;
        }
        for (final CharSequence name : names) {
            final VariableInfo axis = findVariableInfo(name.toString());
            if (axis == null) {
                dimensions.clear();
                axes.clear();
                break;
            }
            axes.add(axis);
            Collections.addAll(dimensions, axis.dimensions);
        }
        return true;
    }

    @Override
    public Grid[] getGridCandidates() throws IOException, DataStoreException {
        if (gridGeometries == null) {
            /*
             * First, find all variables which are used as coordinate system axis. The keys in the map are
             * the grid dimensions which are the domain of the variable (i.e. the sources of the conversion
             * from grid coordinates to CRS coordinates). For each key there is usually only one value, but
             * more complicated netCDF files (e.g. using two-dimensional localisation grids) also exist.
             */
            final var dimToAxes = new IdentityHashMap<DimensionInfo, Set<VariableInfo>>();
            for (final VariableInfo variable : variables) {
                switch (variable.getRole()) {
                    case COVERAGE:
                    case DISCRETE_COVERAGE: {
                        // If Convention.roleOf(…) overwrote the value computed by VariableInfo,
                        // remember the new value for avoiding to ask again in next loops.
                        variable.isCoordinateSystemAxis = false;
                        break;
                    }
                    case AXIS: {
                        variable.isCoordinateSystemAxis = true;
                        for (final DimensionInfo dimension : variable.dimensions) {
                            CollectionsExt.addToMultiValuesMap(dimToAxes, dimension, variable);
                        }
                    }
                }
            }
            /*
             * For each variable, get its list of axes. More than one variable may have the same list of axes,
             * so we remember the previously created instances in order to share the grid geometry instances.
             */
            final var axes = new LinkedHashSet<VariableInfo>(8);
            final var usedDimensions = new HashSet<DimensionInfo>(8);
            final var shared = new LinkedHashMap<GridInfo,GridInfo>();
nextVar:    for (final VariableInfo variable : variables) {
                if (variable.isCoordinateSystemAxis || variable.dimensions.length == 0) {
                    continue;
                }
                /*
                 * The axes can be inferred in two ways: if the variable contains a "coordinates" attribute,
                 * that attribute lists explicitly the variables to use as axes. Otherwise we have to infer
                 * the axes from the variable dimensions, using the `dimToAxes` map computed at the beginning
                 * of this method. If and only if we can find all axes, we create the GridGeometryInfo.
                 * This is a "all or nothing" operation.
                 */
                axes.clear();
                usedDimensions.clear();
                if (!listAxes(variable.getCoordinateVariables(), axes, usedDimensions)) {
                    listAxes(convention().namesOfAxisVariables(variable), axes, usedDimensions);
                }
                /*
                 * In theory the "coordinates" attribute would enumerate all axes needed for covering all dimensions,
                 * and we would not need to check for variables having dimension names. However, in practice there is
                 * incomplete attributes, so we check for other dimensions even if the above loop did some work.
                 */
                for (int i=0; i < variable.dimensions.length; i++) {                     // Reverse of netCDF order. (Natural)
                    final DimensionInfo dimension = variable.dimensions[i];
                    if (usedDimensions.add(dimension)) {
                        final Set<VariableInfo> axis = dimToAxes.get(dimension);       // Should have only 1 element.
                        if (axis == null) {
                            continue nextVar;
                        }
                        axes.addAll(axis);
                    }
                }

                DimensionInfo[] reversed = Arrays.copyOf(variable.dimensions, variable.dimensions.length);
                for (int i = 0, j = reversed.length - 1; i < j; i++, j--) {
                    DimensionInfo tmp = reversed[i];
                    reversed[i] = reversed[j];
                    reversed[j] = tmp;
                }

                /*
                 * Creates the grid geometry using the given domain and range, reusing existing instance if one exists.
                 * We usually try to preserve axis order as declared in the netCDF file. But if we mixed axes inferred
                 * from the "coordinates" attribute and axes inferred from variable names matching dimension names, we
                 * use axes from "coordinates" attribute first followed by other axes.
                 */
                GridInfo grid = new GridInfo(reversed, axes.toArray(VariableInfo[]::new));
                GridInfo existing = shared.putIfAbsent(grid, grid);
                if (existing != null) {
                    grid = existing;
                }
                variable.grid = grid;
            }
            gridGeometries = shared.values().toArray(Grid[]::new);
        }
        return gridGeometries;
    }

    /**
     * Returns the dimension of the given name (eventually ignoring case), or {@code null} if none.
     * This method searches in all dimensions found in the zarr tree structure, regardless of variables.
     * The search will ignore case only if no exact match is found for the given name.
     *
     * @param  dimName  the name of the dimension to search.
     * @return dimension of the given name, or {@code null} if none.
     */
    @Override
    @SuppressWarnings("StringEquality")
    protected Dimension findDimension(final String dimName) {
        DimensionInfo dim = dimensionMap.get(dimName);          // Give precedence to exact match before to ignore case.
        if (dim == null) {
            final String lower = dimName.toLowerCase(Decoder.DATA_LOCALE);
            if (lower != dimName) {                             // Identity comparison is okay here.
                dim = dimensionMap.get(lower);
            }
        }
        return dim;
    }

    /**
     * Returns the netCDF variable of the given name, or {@code null} if none.
     *
     * @param  name  the name of the variable to search, or {@code null}.
     * @return the variable of the given name, or {@code null} if none.
     */
    @SuppressWarnings("StringEquality")
    private VariableInfo findVariableInfo(final String name) {
        VariableInfo v = variableMap.get(name);
        if (v == null && name != null) {
            final String lower = name.toLowerCase(Decoder.DATA_LOCALE);
            // Identity comparison is ok since following check is only an optimization for a common case.
            if (lower != name) {
                v = variableMap.get(lower);
            }
        }
        return v;
    }

    /**
     * Returns the netCDF variable of the given name, or {@code null} if none.
     *
     * @param  name  the name of the variable to search, or {@code null}.
     * @return the variable of the given name, or {@code null} if none.
     */
    @Override
    protected Variable findVariable(final String name) {
        return findVariableInfo(name);
    }

    /**
     * Returns the variable of the given name. Note that groups do not exist in netCDF 3.
     *
     * @param  name  the name of the variable to search, or {@code null}.
     * @return the variable of the given name, or {@code null} if none.
     */
    @Override
    protected Node findNode(final String name) {
        return findVariableInfo(name);
    }

    @Override
    public void close(DataStore lock) throws IOException {
//        input.channel.close();
    }
}
