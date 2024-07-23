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
 * distributed under the License is distributed on an "AS IS" BASIS,z
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sis.storage.gimi.isobmff.iso23001_17;

import java.io.IOException;
import org.apache.sis.io.stream.ChannelDataInput;
import org.apache.sis.storage.gimi.isobmff.FullBox;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class ComponentPalette extends FullBox{

    public static final String FCC = "cpal";

    public static class Component {
        public int componentIndex;
        public int componentBitDepthMinusOne;
        public int componentFormat;
    }
    public Component[] components;
    public int[][] values;

    @Override
    public void readProperties(ChannelDataInput cdi) throws IOException {
        components = new Component[cdi.readUnsignedShort()];
        for (int i = 0; i < components.length; i++) {
            components[i] = new Component();
            components[i].componentIndex = cdi.readInt();
            components[i].componentBitDepthMinusOne = cdi.readUnsignedByte();
            components[i].componentFormat = cdi.readUnsignedByte();
        }
        values = new int[cdi.readInt()][components.length];
        for (int k = 0; k < values.length; k++) {
            for (int i = 0; i < components.length; i++) {
                values[k][i] = (int) cdi.readBits(components[i].componentBitDepthMinusOne+1);
                cdi.skipRemainingBits();
            }
        }
    }

}
