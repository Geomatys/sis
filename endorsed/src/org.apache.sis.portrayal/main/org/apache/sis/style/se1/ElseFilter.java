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
package org.apache.sis.style.se1;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlRootElement;


/**
 * The element to marshall when no other rule matches the conditions.
 *
 * @author  Martin Desruisseaux (Geomatys)
 */
@XmlType(name = "ElseFilterType")
@XmlRootElement(name = "ElseFilter")
final class ElseFilter {
    /**
     * The singleton instance.
     */
    static final ElseFilter INSTANCE = new ElseFilter();

    /**
     * Creates the singleton instance.
     */
    private ElseFilter() {
    }

    /**
     * Returns a string representation of this element.
     */
    @Override
    public String toString() {
        return "ElseFilter";
    }
}
