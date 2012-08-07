package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.document.*;
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
        File file = new File("src/test/resources/data/freebsd.txt");
        final Document doc = new Document();
        doc.add(new IntField(PRIMARY_KEY.toString(), 123, Field.Store.NO));
        doc.add(new StringField(FILE_NAME.name(), file.getName(), Field.Store.NO));
        doc.add(new TextField(FILE_CONTENT.name(), new FileReader(file), Field.Store.NO));
        return doc;
    }

    @Test
    public void storeSequenceFileForDocument() throws Exception {
        MemoryIndexWriter indexWriter = new MemoryIndexWriter(fs, HTU.getHBaseAdmin(), PRIMARY_KEY.toString());
        indexWriter.addDocument(getDoc());
        HTablePool hTablePool = new HTablePool(fs.getConf(), 2);
        MemoryIndexReader reader = new MemoryIndexReader(hTablePool, INDEX_TABLE.toString(), PRIMARY_KEY.toString());
        reader.document(123);
        assertEquals(1, reader.numDocs());
    }
}

