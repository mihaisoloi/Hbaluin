package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.FILE_CONTENT;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.FILE_NAME;
import static org.junit.Assert.assertEquals;

public class HBaseFileSearchIndexTest {

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
        doc.add(new StringField(FILE_NAME.name(), file.getName(), Field.Store.YES));
        doc.add(new TextField(FILE_CONTENT.name(), new FileReader(file), Field.Store.NO));
        return doc;
    }

    @Test
    public void storeSequenceFileForDocument() throws Exception{
        MemoryIndexWriter indexWriter = new MemoryIndexWriter(fs,HTU.getHBaseAdmin(),getDoc());
        indexWriter.writeDocument();
    }
}

