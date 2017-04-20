/*
 * Copyright 2017 fdefalco.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ohdsi.webapi.search;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 *
 * @author fdefalco
 */
public class FullTextIndex {
    private static Directory vocabularyDirectory;

    public Directory getDirectory() {
        if (vocabularyDirectory == null) {
            vocabularyDirectory = new RAMDirectory();
        }
        
        return vocabularyDirectory;
    }
}
