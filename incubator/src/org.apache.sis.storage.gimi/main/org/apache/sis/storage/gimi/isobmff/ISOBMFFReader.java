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
package org.apache.sis.storage.gimi.isobmff;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.sis.io.stream.ChannelDataInput;
import org.apache.sis.storage.IllegalNameException;
import org.apache.sis.storage.gimi.isobmff.iso14496_12.Extension;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class ISOBMFFReader {

    private static final Set<BoxRegistry> REGISTRIES;
    private static final Map<String,BoxRegistry> INDEX = new HashMap<>();
    private static final Map<String,BoxRegistry> INDEX_EXT = new HashMap<>();

    static {
        final ServiceLoader<BoxRegistry> loader = ServiceLoader.load(BoxRegistry.class);
        final Iterator<BoxRegistry> iterator = loader.iterator();
        final Set<BoxRegistry> registries = new HashSet<>();
        while (iterator.hasNext()) {
            final BoxRegistry registry = iterator.next();
            registries.add(registry);
            for (String fcc : registry.getBoxesFourCC()) {
                INDEX.put(fcc, registry);
            }
            for (String uuid : registry.getExtensionUUIDs()) {
                INDEX_EXT.put(uuid, registry);
            }
        }
        REGISTRIES = Set.copyOf(registries);
    }

    public static Set<BoxRegistry> getRegistries() {
        return REGISTRIES;
    }

    public static Box newBox(int fourCC, String extensionUuid) {
        return newBox(Box.intToFourCC(fourCC), extensionUuid);
    }

    public static Box newBox(String fourCC, String extensionUuid) {
        if (fourCC.equals("uuid")) {
            //extension
            final BoxRegistry registry = INDEX_EXT.get(extensionUuid.toString());
            if (registry == null) return new Extension();
            try {
                return registry.createExtension(extensionUuid);
            } catch (IllegalNameException ex) {
                //should not happen since we indexed registries
                throw new IllegalStateException("Registry " + registry.getName() +" declares supporting " + extensionUuid.toString() +" but fails to create it. It must be fixed.");
            }

        } else {
            final BoxRegistry registry = INDEX.get(fourCC);
            if (registry == null) return new Box();
            try {
                return registry.create(fourCC);
            } catch (IllegalNameException ex) {
                //should not happen since we indexed registries
                throw new IllegalStateException("Registry " + registry.getName() +" declares supporting " + fourCC +" but fails to create it. It must be fixed.");
            }
        }
    }


    public final ChannelDataInput channel;

    public ISOBMFFReader(ChannelDataInput input) {
        this.channel = input;
    }

    /**
     * Read next box header but not it's content.
     * @return empty box
     */
    public Box readBox() throws IOException {
        return readBox(channel);
    }

    /**
     * Read next box header but not it's content.
     * @return empty box
     */
    public static Box readBox(ChannelDataInput channel) throws IOException {
        channel.buffer.order(ByteOrder.BIG_ENDIAN);
        long offset = channel.getStreamPosition();

        //read box header
        long size = ((long)channel.readInt()) & 0xFFFFFFFFl;
        final String type = Box.intToFourCC(channel.readInt());
        UUID uuid = null;
        if (size == 1l) size = channel.readLong();
        if ("uuid".equals(type)) uuid = new UUID(channel.readLong(), channel.readLong());

        final Box box = newBox(type, String.valueOf(uuid));
        box.boxOffset = offset;
        box.type = type;
        box.size = size;
        box.uuid = uuid;

        //read full box header
        if (box instanceof FullBox) {
            final FullBox fb = (FullBox) box;
            fb.version = channel.readUnsignedByte();
            fb.flags = (channel.readUnsignedByte() << 16) |
                        (channel.readUnsignedByte() << 8) |
                        channel.readUnsignedByte();
        }
        box.payloadOffset = channel.getStreamPosition();

        return box;
    }

    public static String readUtf8String(ChannelDataInput cdi) throws IOException {
        long start = cdi.getStreamPosition();
        int size = 0;
        while (cdi.readByte() != 0) {
            size++;
        }
        cdi.seek(start);
        String str = cdi.readString(size, StandardCharsets.UTF_8);
        cdi.readByte(); //skip string 0/null terminal marker
        return str;
    }

    /**
     * Load box payload and all children payload recursively.
     */
    public static void load(Box box, ChannelDataInput cdi) throws IOException {
        box.readPayload(cdi);
        for (Box b : box.getChildren(cdi)) {
            load(b, cdi);
        }
    }
}
