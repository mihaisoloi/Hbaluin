package org.apache.lucene.index;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.lucene.hbase.HBaseNames;
import org.apache.lucene.document.*;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;

public class MemoryIndexReader extends AtomicReader {

    private final HBaseIndexStore store;
    private final HTablePool tablePool;
    private final static Logger LOG = LoggerFactory.getLogger(MemoryIndexReader.class);

    public String getIndexName() {
        return indexName;
    }

    private final String indexName;

    /**
     * @param store to be used by the index reader
     * @param indexName  Name of the index to be read from. (i.e. tableName)
     */
    public MemoryIndexReader(final HBaseIndexStore store, final String indexName) {
        this.store = store;
        this.tablePool = store.getTablePool();
        this.indexName = indexName;
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int numDocs() {
        HTableInterface table = tablePool.getTable(indexName);
        HashSet<Integer> documentSet = Sets.newHashSet();
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        scan.addFamily(HBaseNames.COLUMN_FAMILY.name);
        try {
            for (Result result : table.getScanner(scan)) {
                for (KeyValue qualifier : result.list()){
                    documentSet.add(Bytes.toInt(qualifier.getQualifier()));
                }
            }
        } catch (IOException e) {
            LOG.warn("Error in numDocs()", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                //do nothing
            }
        }
        return documentSet.size();
    }

    @Override
    public int maxDoc() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        HTableInterface table = tablePool.getTable(indexName);
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);

        Document doc = ((DocumentStoredFieldVisitor) visitor).getDocument();
        doc.add(new IntField(PRIMARY_KEY.toString(), docID, Field.Store.NO));
        StringBuilder termBuilder = new StringBuilder();
        for (Result result : table.getScanner(scan)) {
            String row = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> documentIds = result.getFamilyMap(COLUMN_FAMILY.name);
            for (byte[] docId : documentIds.keySet()) {
                if (docID == Bytes.toInt(docId)) {
                    List<KeyValue> fields = result.getColumn(COLUMN_FAMILY.name, docId);
                    for (KeyValue field : fields){
                        String fieldName = Bytes.toString(field.getValue());
                        String term = row.substring(row.indexOf("-") + 1);
                        if (fieldName.equals(FILE_NAME.toString()))
                            doc.add(new StringField(FILE_NAME.name(), term, Field.Store.NO));
                        else if (fieldName.equals(FILE_CONTENT.name()))
                            termBuilder.append(row.substring(row.indexOf("/") + 1));
                    }
                }
                doc.add(new TextField(FILE_CONTENT.name(), termBuilder.toString(), Field.Store.NO));
            }
        }
        table.close();
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
