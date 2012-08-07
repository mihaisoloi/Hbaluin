package org.apache.lucene.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.INDEX_TABLE;

public class HBaseIndexStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexStore.class);

    public HTablePool getTablePool() {
        return tablePool;
    }

    private HTablePool tablePool;
    private HTableInterface table;
    private final Configuration configuration;

    public HBaseIndexStore(final HTablePool tablePool,
                           final Configuration configuration, final String indexName)
            throws IOException {
        this.table = tablePool.getTable(indexName);
        this.configuration = configuration;
    }

    public HTableInterface getTable() {
        return table;
    }

    public static HTableInterface createIndexTable(final Configuration configuration) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);

        HTableDescriptor htd = new HTableDescriptor(INDEX_TABLE.name);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(COLUMN_FAMILY.name);
        htd.addFamily(columnDescriptor);
        admin.createTable(htd);

        return new HTable(configuration, INDEX_TABLE.name);
    }

    public Put persistTerm(String mailboxId, int docId, String field, byte[] term) throws IOException {
        //row = mailboxID - term
        Put put = new Put(Bytes.add(Bytes.toBytes(mailboxId),Constants.SEPARATOR,term));
        // family=column_family, qualifier = documentID,value = fields
        put.add(COLUMN_FAMILY.name, Bytes.toBytes(docId), Bytes.toBytes(field));
        return put;
    }
}
