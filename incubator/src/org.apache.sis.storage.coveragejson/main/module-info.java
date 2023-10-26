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

/**
 * Coverage-json store.
 *
 * @author  Johann Sorel (Geomatys)
 */
module org.apache.sis.storage.coveragejson {
    requires transitive org.apache.sis.storage;
    requires transitive jakarta.json.bind;
    requires transitive jakarta.json;
    requires transitive org.eclipse.yasson;

    provides org.apache.sis.storage.DataStoreProvider
            with org.apache.sis.storage.coveragejson.CoverageJsonStoreProvider;

    exports org.apache.sis.storage.coveragejson;
    //should not be exposed but is needed for yasson to find classes and methods.
    exports org.apache.sis.storage.coveragejson.binding;
}
