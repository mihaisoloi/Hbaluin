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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class Searcher {

    public static void main(String[] args) throws IOException, ParseException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: java "
                    + Searcher.class.getName() + " <index dir> <query>");
        }

        String indexDir = args[0];
        String q = args[1];
        search(indexDir, q);
    }

    private static void search(String indexDir, String q) throws IOException,
            ParseException {
        FSDirectory dir = FSDirectory.open(new File(indexDir));
        IndexSearcher is = null;
        try {
            is = new IndexSearcher(IndexReader.open(dir));

            QueryParser parser = new QueryParser(LUCENE_36, "contents",
                    new StandardAnalyzer(LUCENE_36));
            Query query = parser.parse(q);
            long start = System.currentTimeMillis();
            TopDocs hits = is.search(query, 10);
            long end = System.currentTimeMillis();

            System.err.println("Found " + hits.totalHits + " document(s) (in "
                    + (end - start) + " milliseconds) that matched query '" + q
                    + "':");

            for (ScoreDoc scoreDoc : hits.scoreDocs) {
                Document doc = is.doc(scoreDoc.doc);
                System.out.println(doc.get("fullpath"));
            }
        } finally {
            if (is != null)
                is.close();
        }
    }
}
