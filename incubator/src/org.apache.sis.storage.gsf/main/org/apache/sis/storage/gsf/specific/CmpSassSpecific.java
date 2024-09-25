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
package org.apache.sis.storage.gsf.specific;

import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;
import org.apache.sis.storage.gsf.GSF;
import org.apache.sis.storage.gsf.StructClass;


/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class CmpSassSpecific extends StructClass {
    private static final OfDouble LAYOUT_LFREQ;
    private static final OfDouble LAYOUT_LNTENS;
    public static final GroupLayout LAYOUT = MemoryLayout.structLayout(
        LAYOUT_LFREQ = GSF.C_DOUBLE.withName("lfreq"),
        LAYOUT_LNTENS = GSF.C_DOUBLE.withName("lntens")
    ).withName("t_gsfCmpSassSpecific");

    public CmpSassSpecific(MemorySegment struct) {
        super(struct);
    }

    @Override
    protected MemoryLayout getLayout() {
        return LAYOUT;
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * double lfreq
     * }
     */
    public double getLfreq() {
        return struct.get(LAYOUT_LFREQ, 0);
    }

    /**
     * Getter for field:
     * {@snippet lang=c :
     * double lntens
     * }
     */
    public double getLntens() {
        return struct.get(LAYOUT_LNTENS, 8);
    }
}
