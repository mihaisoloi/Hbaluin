package org.apache.lucene.index;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.lucene.hbase.HBaseNames;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;

public class MemoryIndexReader extends AtomicReader {

    private final HTablePool hTablePool;
    private final String indexName;
    private final byte[] primaryKeyField;

    /**
     * @param hTablePool TablePool to be used by the index reader
     * @param indexName  Name of the index to be read from.
     */
    public MemoryIndexReader(final HTablePool hTablePool, final String indexName, final String primaryKeyField) {
        this.hTablePool = hTablePool;
        this.indexName = indexName;
        this.primaryKeyField = Bytes.toBytes(primaryKeyField);
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int numDocs() {
        HTableInterface table = hTablePool.getTable(indexName);
        int numDocs = 0;
        try {
            Scan scan = new Scan();
            scan.addColumn(HBaseNames.COLUMN_FAMILY.name,
                    HBaseNames.CONTENTS_QUALIFIER.name);
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();
            Set<String> documentSet = Sets.newHashSet();
            while (result != null) {
                String row = Bytes.toString(result.getRow());
                documentSet.add(row.substring(0, row.indexOf("-")));
                result = resultScanner.next();
            }
            numDocs = documentSet.size();
        } catch (IOException e) {
            System.err.println("Error in numDocs() " + e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                //do nothing
            }
        }
        return numDocs;
    }

    @Override
    public int maxDoc() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        HTableInterface hTable = hTablePool.getTable(indexName);
        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY.name, HBaseNames.CONTENTS_QUALIFIER.name);
        Document doc = ((DocumentStoredFieldVisitor) visitor).getDocument();
        ResultScanner resultScanner = hTable.getScanner(scan);
        Result result = resultScanner.next();
        while (result != null) {
            String key = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> myMap = result.getFamilyMap(COLUMN_FAMILY.name);
            for (byte[] qualifier : myMap.keySet())
                doc.add(new StringField(key, Bytes.toString(qualifier), Field.Store.NO));
            result = resultScanner.next();
        }
        hTable.close();
    }

    @Override
    public boolean hasDeletions() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doClose() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Fields fields() throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DocValues docValues(String field) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DocValues normValues(String field) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FieldInfos getFieldInfos() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Bits getLiveDocs() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
