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
package org.apache.sis.referencing;

import org.opengis.util.InternationalString;
import org.opengis.metadata.citation.Citation;
import org.apache.sis.metadata.iso.ImmutableIdentifier;
import org.apache.sis.util.Deprecable;


/**
 * An identifier which should not be used anymore.
 * This is used mostly for deprecated EPSG codes.
 *
 * <div class="note"><b>Implementation note:</b>
 * this class opportunistically recycle the {@linkplain #getDescription() description} property into a
 * {@linkplain #getRemarks() remarks} property. This is a lazy way to inherit {@link #equals(Object)}
 * and {@link #hashCode()} implementation without adding code in this class for taking in account a
 * new field.</div>
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @since   0.6
 * @version 0.6
 * @module
 */
final class DeprecatedCode extends ImmutableIdentifier implements Deprecable {
    /**
     * For cross-version compatibility.
     */
    private static final long serialVersionUID = 357222258307746767L;

    /**
     * Creates a deprecated identifier.
     */
    DeprecatedCode(final Citation authority, final String codeSpace,
            final String code, final String version, final InternationalString remarks)
    {
        super(authority, codeSpace, code, version, remarks);
    }

    /**
     * Returns {@code true} since this identifier is deprecated.
     *
     * @return {@code true}.
     */
    @Override
    public boolean isDeprecated() {
        return true;
    }

    /**
     * Information about the replacement for this identifier.
     *
     * <div class="note"><b>Example:</b> "superseded by code XYZ".</div>
     *
     * @return Information about the replacement for this identifier, or {@code null} if none.
     */
    @Override
    public InternationalString getRemarks() {
        return super.getDescription();
    }

    /**
     * Returns {@code null}, since we used the description for the superseded information.
     * See <cite>"Implementation note"</cite> in class javadoc.
     *
     * @return {@code null}.
     */
    @Override
    public InternationalString getDescription() {
        return null;
    }
}
