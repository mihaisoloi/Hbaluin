package org.apache.lucene.index;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

public class HBaseIndexStore {

    private final HTableInterface table;

    public HBaseIndexStore(final HTablePool tablePool,
                           final Configuration configuration, final String indexName)
            throws IOException {
        this.table = tablePool.getTable(indexName);

    }
}
