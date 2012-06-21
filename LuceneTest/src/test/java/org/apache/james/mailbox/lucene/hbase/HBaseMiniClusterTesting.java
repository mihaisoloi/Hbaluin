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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.ftpserver.util.IoUtils;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.avro.AvroUtil;
import org.apache.hadoop.hbase.avro.generated.AClusterStatus;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.james.mailbox.lucene.avro.AvroUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.NavigableMap;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;
import static org.apache.lucene.util.Version.LUCENE_36;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseMiniClusterTesting extends HBaseSetup {

    private Schema termDocument;

    @Before
    public void setUp() throws IOException {
        termDocument = AvroUtils
                .parseSchema(new File("LuceneTest/resources/main/avro/TermDocument.avro"));
    }

    @Test
    public void insertDataIntoHBaseNodes() throws IOException {
        assertTrue(admin.tableExists(INDEX_TABLE.name));
        HTableDescriptor htd = admin.getTableDescriptor(INDEX_TABLE.name);
        assertTrue(htd.getFamiliesKeys().contains(COLUMN_FAMILY.name));
    }

    @Test
    public void insertValuesIntoColumns() throws IOException {
        HTable htable = new HTable(CLUSTER.getConf(), INDEX_TABLE.name);
        Put put = new Put(toBytes("mihai"));
        put.add(COLUMN_FAMILY.name, toBytes("varsta"), toBytes(27));
        put.add(COLUMN_FAMILY.name, toBytes("sex"), toBytes("male"));
        htable.put(put);
        htable.flushCommits();
        Get get = new Get(toBytes("mihai"));
        get.addFamily(COLUMN_FAMILY.name);
        Result result = htable.get(get);
        NavigableMap<byte[], byte[]> myMap = result
                .getFamilyMap(COLUMN_FAMILY.name);
        assertEquals(27, Bytes.toInt(myMap.get(toBytes("varsta"))));
        assertEquals("male", Bytes.toString(myMap.get(toBytes("sex"))));
        htable.close();
    }

    @Test
    public void testAvroHBaseIntegration() throws IOException {
        ClusterStatus cs = admin.getClusterStatus();
        assertEquals(1, cs.getServersSize());
        AClusterStatus acs = AvroUtil.csToACS(cs);
        assertEquals(cs.getServersSize(), acs.servers);
    }

    @Test
    public void insertAvroBLOBIntoColumns() throws IOException {
        //constructing the avro blob
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get()
                .binaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                termDocument);
        GenericRecord genericRecord = new GenericData.Record(termDocument);
        genericRecord.put("docFrequency", 2);
        genericRecord.put("docPositions", new int[]{1, 2, 3, 4, 5});

        writer.write(genericRecord, encoder);

        encoder.flush();

        //inserting the blob into the HBase table
        HTable htable = new HTable(CLUSTER.getConf(), INDEX_TABLE.name);
        Put put = new Put(toBytes("termDocument"));
        put.add(TERM_DOCUMENT_CF.name, toBytes("avro"), outputStream.toByteArray());

        //getting it back
        Get get = new Get(toBytes("termDocument"));
        get.addFamily(TERM_DOCUMENT_CF.name);
        Result result = htable.get(get);
        NavigableMap<byte[], byte[]> myMap = result
                .getFamilyMap(TERM_DOCUMENT_CF.name);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(myMap.get(toBytes("avro")));
        htable.close();

        //decoding it
        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                termDocument);

        while (true) {
            try {
                GenericRecord record = reader.read(null, decoder);
                System.out.println(record);
            } catch (EOFException eof) {
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        IoUtils.close(inputStream);
        IoUtils.close(outputStream);
    }

    @Test
    public void insertSegmentsIntoColumns() throws IOException, ParseException {
        //indexing the text files and inserting the segments into the HBase cluster
        String dataDir = "LuceneTest/resources/main/data";
        Indexer indexer = new Indexer(CLUSTER.getConf());
        int numFileIndexed;
        try {
            numFileIndexed = indexer.index(dataDir, new Indexer.TextFilesFilter());
            System.out.println("Number of files indexed and stored in HBase: "+numFileIndexed);
        } finally {
            indexer.close();
        }

        IndexReader reader = IndexReader.open(indexer.dir);
        IndexSearcher is = new IndexSearcher(reader);

        QueryParser parser = new QueryParser(LUCENE_36, "contents",
                new StandardAnalyzer(LUCENE_36));
        Query query = parser.parse("apache");


        TopDocs hits = is.search(query, 10);

        for (ScoreDoc scoreDoc : hits.scoreDocs) {
            Document doc = is.doc(scoreDoc.doc);
            System.out.println(doc.get("fullpath"));
        }
    }

}
