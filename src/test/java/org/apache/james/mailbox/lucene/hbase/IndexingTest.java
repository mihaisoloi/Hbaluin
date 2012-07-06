/******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more         *
 * contributor license agreements. See the NOTICE file distributed with       *
 * this work for additional information regarding copyright ownership.        *
 * The ASF licenses this file to You under the Apache License, Version 2.0    *
 * (the "License"); you may not use this file except in compliance with       *
 * the License. You may obtain a copy of the License at                       *
 *                                                                            *
 * http://www.apache.org/licenses/LICENSE-2.0                                 *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the License is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 ******************************************************************************/

package org.apache.james.mailbox.lucene.hbase;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.lucene.util.Version.LUCENE_40;
import static org.junit.Assert.assertEquals;

public class IndexingTest /*extends HBaseSetup*/ {
    protected static final String[] ids = {"1", "2"};
    protected static final String[] unindexed = {"Netherlands", "Italy"};
    protected static final String[] unstored = {"Amsterdam has lots of bridges",
            "Venice has lots of canals"};
    protected static final String[] text = {"Amsterdam", "Venice"};
    private static Directory directory;
    private static IndexWriter writer;

    /**
     * setting up the test class
     *
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUp() throws IOException {
//        super.setUp();
        directory = new HBaseDirectory(/*CLUSTER.getConf()*/);

        writer = getWriter();
        for (int i = 0; i < ids.length; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", ids[i], Store.YES));
            doc.add(new StringField("country", unindexed[i], Store.YES));
            doc.add(new StringField("city", text[i], Store.YES));
            doc.add(new TextField("contents", unstored[i], Store.NO));
            writer.addDocument(doc);
        }
//        writer.commit();
    }

    /**
     * instantiates the IndexWriter
     *
     * @return
     * @throws CorruptIndexException
     * @throws LockObtainFailedException
     * @throws IOException
     */
    private static IndexWriter getWriter() throws IOException {
        IndexWriterConfig conf = new IndexWriterConfig(LUCENE_40,
                new WhitespaceAnalyzer(LUCENE_40));
        return new IndexWriter(directory, conf);
    }

    /**
     * instantiates the IndexReader
     *
     * @return
     * @throws CorruptIndexException
     * @throws IOException
     */
    private IndexReader getReader() throws IOException {
        return DirectoryReader.open(directory);
    }

    /**
     * tests the IndexWriter
     *
     * @throws CorruptIndexException
     * @throws IOException
     */
    @Test
    public void testIndexWriter() throws IOException {
        assertEquals(ids.length, writer.numDocs());
        writer.close(true);
    }

    /**
     * tests the IndexReader
     *
     * @throws IOException
     */
    @Test
    public void testIndexReader() throws IOException {
        IndexReader reader = getReader();
        assertEquals(ids.length, reader.maxDoc());
        assertEquals(ids.length, reader.numDocs());
        reader.close();
    }

}
