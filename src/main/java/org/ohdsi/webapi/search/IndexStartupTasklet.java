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

import java.io.IOException;
import java.sql.ResultSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.ohdsi.webapi.service.VocabularyService;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.RowCallbackHandler;

/**
 *
 * @author fdefalco
 */
public class IndexStartupTasklet implements Tasklet {

    private StandardAnalyzer analyzer;
    private IndexWriterConfig indexWriterConfig;
    private IndexWriter indexWriter;
    private final VocabularyService vocabularyService;

    public IndexStartupTasklet(VocabularyService service) {
        vocabularyService = service;
    }

    private RowCallbackHandler rch = (ResultSet rs) -> {
        Document document = new Document();

        Field name = new Field("concept_name", rs.getString("CONCEPT_NAME"), TextField.TYPE_STORED);
        Field id = new Field("concept_id", rs.getString("CONCEPT_ID"), TextField.TYPE_STORED);
        Field standard = new Field("standard_concept", rs.getString("STANDARD_CONCEPT"), TextField.TYPE_STORED);
        Field invalid = new Field("invalid_reason", rs.getString("INVALID_REASON"), TextField.TYPE_STORED);
        Field code = new Field("concept_code", rs.getString("CONCEPT_CODE"), TextField.TYPE_STORED);
        Field conceptClass = new Field("concept_class", rs.getString("CONCEPT_CLASS_ID"), TextField.TYPE_STORED);
        Field domain = new Field("domain_id", rs.getString("DOMAIN_ID"), TextField.TYPE_STORED);
        Field vocabulary = new Field("vocabulary_id", rs.getString("VOCABULARY_ID"), TextField.TYPE_STORED);
               
        document.add(name);
        document.add(id);
        document.add(standard);
        document.add(invalid);
        document.add(code);
        document.add(conceptClass);
        document.add(domain);
        document.add(vocabulary);
        
        try {
            indexWriter.addDocument(document);
        } catch (IOException ioe) {
            log.debug(ioe);
        }
    };

    private static final Log log = LogFactory.getLog(IndexStartupTasklet.class);
    
    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        FullTextIndex fti = new FullTextIndex();
        Directory index = fti.getDirectory();
        analyzer = new StandardAnalyzer();
        indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriter = new IndexWriter(index, indexWriterConfig);
        vocabularyService.processConcepts(this.rch);
        indexWriter.commit();
        indexWriter.close();
        return RepeatStatus.FINISHED;
    }
}
