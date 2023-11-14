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
package org.apache.sis.storage.shapefile.shp;

import java.awt.geom.Rectangle2D;
import org.apache.sis.io.stream.ChannelDataInput;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;
import org.apache.sis.geometry.Envelope2D;

/**
 * Seekable shape file reader.
 *
 * @author Johann Sorel (Geomatys)
 */
public final class ShapeReader implements AutoCloseable{

    private final ChannelDataInput channel;
    private final ShapeHeader header;
    private final ShapeGeometryEncoder geomParser;
    private final Rectangle2D.Double filter;

    public ShapeReader(ChannelDataInput channel, Rectangle2D.Double filter) throws IOException {
        Objects.nonNull(channel);
        this.channel = channel;
        this.filter = filter;
        header = new ShapeHeader();
        header.read(channel);
        geomParser = ShapeGeometryEncoder.getEncoder(header.shapeType);
    }

    public ShapeHeader getHeader() {
        return header;
    }

    public void moveToOffset(long position) throws IOException {
        channel.seek(position);
    }

    public ShapeRecord next() throws IOException {
        final ShapeRecord record = new ShapeRecord();
        try {
            //read until we find a record matching the filter or EOF exception
            //we do not trust EOF exception, some channel implementations with buffers may continue to say they have datas
            //but they are picking in an obsolete buffer.
            for (;;) {
                if (header.fileLength <= channel.getStreamPosition()) {
                    return null;
                }
                if (record.read(channel, geomParser, filter)) break;
            }
            return record;
        } catch (EOFException ex) {
            //no more records
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        channel.channel.close();
    }
}