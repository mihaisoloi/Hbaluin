package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;

/**
 * scrie o instanta de Lucene Document in HBase
 */
public class MemoryIndexWriter {

    private Document doc;
    private FileSystem fs;
    private byte[] column = HBaseNames.COLUMN_FAMILY.name;

    public MemoryIndexWriter(FileSystem fs, HBaseAdmin admin, Document doc) throws IOException {
        this.fs = fs;
        this.doc = doc;

        HTableDescriptor htd = new HTableDescriptor(HBaseNames.INDEX_TABLE.name);
        if (!htd.hasFamily(column)) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
            htd.addFamily(columnDescriptor);
        }
        if (!admin.tableExists(HBaseNames.INDEX_TABLE.name)) {
            admin.createTable(htd);
        }
    }

    public void writeDocument() throws IOException {
        HTable table = null;
        try {
            table = new HTable(fs.getConf(), HBaseNames.INDEX_TABLE.name);
            Put put = new Put(Bytes.toBytes(doc.get(HBaseNames.FILE_NAME.name())));

            for (IndexableField field : doc.getFields()) {
                // family=mailboxID, qualifier = numele term-ului,value = document positions/ frequency --> should be TermDocument
                put.add(column, Bytes.toBytes(field.name()), Bytes.toBytes(1));

            }
            table.put(put);
        } finally {
            table.flushCommits();
        }
    }

    private int getFrequency(IndexableField field) {
        int i = 0;
        for (IndexableField documentField : doc.getFields()) {
            if (documentField.equals(field))
                i++;
        }
        return i;
    }
}
