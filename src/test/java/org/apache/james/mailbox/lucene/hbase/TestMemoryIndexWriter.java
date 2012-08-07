package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.document.*;
import org.apache.lucene.index.HBaseIndexStore;
import org.apache.lucene.index.MemoryIndexReader;
import org.apache.lucene.index.MemoryIndexWriter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;
import static org.junit.Assert.assertEquals;

public class TestMemoryIndexWriter {

    private static HBaseTestingUtility HTU = new HBaseTestingUtility();
    private static FileSystem fs;
    private static final File file = new File("src/test/resources/data/freebsd.txt");

    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        HTU.startMiniCluster();
    }

    @Before
    public void setUp() throws IOException {
        fs = HTU.getTestFileSystem();
    }

    @Test
    public void createDocument() throws IOException {
        assertEquals("freebsd.txt", getDoc().getField(HBaseNames.FILE_NAME.name()).stringValue());
    }

    private Document getDoc() throws IOException {
        final Document doc = new Document();
        doc.add(new IntField(PRIMARY_KEY.toString(), 123, Field.Store.NO));
        doc.add(new StringField(FILE_NAME.toString(), file.getName(), Field.Store.NO));
        doc.add(new TextField(FILE_CONTENT.toString(), new FileReader(file), Field.Store.NO));
        return doc;
    }

    @Test
    public void storeSequenceFileForDocument() throws Exception {
        HTablePool hTablePool = new HTablePool(fs.getConf(), 2);
        HBaseIndexStore.createIndexTable(fs.getConf());
        HBaseIndexStore storage = new HBaseIndexStore(hTablePool,fs.getConf(),INDEX_TABLE.toString());

        MemoryIndexWriter indexWriter = new MemoryIndexWriter(storage, PRIMARY_KEY.toString());
        indexWriter.storeMail(getDoc());

        MemoryIndexReader reader = new MemoryIndexReader(hTablePool, INDEX_TABLE.toString());
        //asta este al nspelea termen
        Document doc = reader.document(123);

        assertEquals(1, reader.numDocs());
        assertEquals(file.getName(),doc.get(FILE_NAME.toString()));
        System.out.println(doc.get(FILE_CONTENT.toString()));
    }
}

