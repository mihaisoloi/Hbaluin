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

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.lucene.util.Version.LUCENE_40;

public class Indexer {
    private final IndexWriter writer;
    public final HBaseDirectory dir;

    public Indexer(Configuration conf) throws IOException {
        dir = new HBaseDirectory();
        writer = new IndexWriter(dir, createConfig(false, true));
    }

    private IndexWriterConfig createConfig(boolean doMerge, boolean defaultMerge) {
        IndexWriterConfig config = new IndexWriterConfig(LUCENE_40,
                new StandardAnalyzer(LUCENE_40));
        //the default merge policy for LUCENE_32 and above is TieredMergePolicy
        //which already has useCompoundFile set to true
        if (doMerge)
            if (defaultMerge)
                ((TieredMergePolicy) config.getMergePolicy()).setNoCFSRatio(1.0);
            else
                config.setMergePolicy(NoMergePolicy.COMPOUND_FILES);

        return config;
    }

    public int index(String dataDir, TextFilesFilter filter)
            throws IOException {
        File[] files = new File(dataDir).listFiles();
        for (File f : files)
            if (!f.isDirectory() && !f.isHidden() && f.exists() && f.canRead()
                    && (filter == null || filter.accept(f)))
                indexFile(f);
        return writer.numDocs();
    }

    public void close() throws IOException {
        writer.close();
    }

    protected Document getDocument(File f) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("contents", new FileReader(f), Field.Store.NO));
        doc.add(new StringField("filename", f.getName(), Field.Store.YES));
        doc.add(new StringField("fullpath", f.getCanonicalPath(), Field.Store.YES));
        return doc;
    }

    private void indexFile(File f) throws IOException {
        System.out.println("Indexing " + f.getCanonicalPath());
        Document doc = getDocument(f);
        writer.addDocument(doc);
    }

    public static class TextFilesFilter implements FileFilter {

        public boolean accept(File pathname) {
            return pathname.getName().toLowerCase().endsWith(".txt");
        }
    }
}
