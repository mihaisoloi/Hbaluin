package org.apache.james.mailbox.lucene.hbase;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.lucene.util.Version.LUCENE_36;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.james.mailbox.lucene.hbase.HBaseDirectory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.Before;
import org.junit.Test;

public class IndexingTest {
    protected String[] ids = { "1", "2" };
    protected String[] unindexed = { "Netherlands", "Italy" };
    protected String[] unstored = { "Amsterdam has lots of bridges",
            "Venice has lots of canals" };
    protected String[] text = { "Amsterdam", "Venice" };
    private Directory directory;

    /**
     * setting up the test class
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        directory = new HBaseDirectory();

        IndexWriter writer = getWriter();

        for (int i = 0; i < ids.length; i++) {
            Document doc = new Document();
            doc.add(new Field("id", ids[i], Store.YES, Index.NOT_ANALYZED));
            doc.add(new Field("country", unindexed[i], Store.YES, Index.NO));
            doc.add(new Field("contents", unstored[i], Store.NO, Index.ANALYZED));
            doc.add(new Field("city", text[i], Store.YES, Index.ANALYZED));
            writer.addDocument(doc);
        }
        writer.close();
    }

    /**
     * instantiates the IndexWriter
     * 
     * @return
     * @throws CorruptIndexException
     * @throws LockObtainFailedException
     * @throws IOException
     */
    private IndexWriter getWriter() throws CorruptIndexException,
            LockObtainFailedException, IOException {
        IndexWriterConfig conf = new IndexWriterConfig(LUCENE_36,
                new WhitespaceAnalyzer(LUCENE_36));
        return new IndexWriter(directory, conf);
    }

    /**
     * instantiates the IndexReader
     * 
     * @return
     * @throws CorruptIndexException
     * @throws IOException
     */
    private IndexReader getReader() throws CorruptIndexException, IOException {
        return IndexReader.open(directory);
    }

    /**
     * returns the number of hits for the searched string
     * 
     * @param fieldName
     * @param searchString
     * @return
     * @throws IOException
     */
    protected int getHitCount(String fieldName, String searchString)
            throws IOException {
        IndexSearcher searcher = new IndexSearcher(getReader());
        Term t = new Term(fieldName, searchString);
        Query query = new TermQuery(t);
        // int hitCount = TestUtil.hitCount(searcher, query);
        searcher.close();
        // return hitCount;
        return 0;
    }

    /**
     * tests the IndexWriter
     * 
     * @throws CorruptIndexException
     * @throws IOException
     */
    @Test
    public void testIndexWriter() throws CorruptIndexException, IOException {
        IndexWriter writer = getWriter();
        assertEquals(ids.length, writer.numDocs());
        writer.close();
    }

    /**
     * tests the IndexReader
     * 
     * @throws IOException
     */
    @Test
    public void testindexReader() throws IOException {
        IndexReader reader = getReader();
        assertEquals(ids.length, reader.maxDoc());
        assertEquals(ids.length, reader.numDocs());
        reader.close();
    }

}
