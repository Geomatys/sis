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
package org.apache.sis.geometries.math;


/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class AbstractTupleArray implements TupleArray {

    @Override
    public TupleArray resize(int newSize) {
        final TupleArray copy = TupleArrays.of(getSampleSystem(), getDataType(), newSize);
        final TupleArrayCursor cursor = cursor();
        while (cursor.next()) {
            final int idx = cursor.coordinate();
            if (idx >= newSize) break;
            copy.set(idx, cursor.samples());
        }
        return copy;
    }

    @Override
    public TupleArray copy() {
        return resize(getLength());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TupleArray)) {
            return false;
        }
        final TupleArray other = (TupleArray) obj;

        final int dim = getDimension();
        if (dim != other.getDimension()) {
            return false;
        }
        final int length = getLength();
        if (length != other.getLength()) {
            return false;
        }
        if (!getSampleSystem().equals(other.getSampleSystem())) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!get(i).equals(other.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getDataType().hashCode() | (getDimension() * 21) | (getLength() * 7);
    }
}
