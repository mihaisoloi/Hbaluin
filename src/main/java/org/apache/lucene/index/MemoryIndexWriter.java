package org.apache.lucene.index;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.lucene.hbase.HBaseNames;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;

/**
 * writes the rows in HBase
 */
public class MemoryIndexWriter {

    private final HBaseIndexStore storage;

    public MemoryIndexWriter(HBaseIndexStore storage) throws IOException {

        this.storage = storage;
    }

    /**
     * writes the rows as puts in HBase where the qualifier is composed of the mailID
     *
     * @param puts
     * @throws IOException
     */
    public void storeMail(List<Put> puts) throws IOException {
        HTableInterface table = storage.getTable();
        for (Put put : puts) {
            table.put(put);
        }
        table.close();
    }

    /**
     * retrieves specific mail from storage
     *
     * @param mailboxId
     * @param messageId
     */
    public ResultScanner retrieveMail(byte[] mailboxId, long messageId) throws IOException {
        HTableInterface table = storage.getTable();
        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public void deleteMail(List<Long> mailIds){
        HTableInterface table = storage.getTable();
    }

    public void flushToStore() throws IOException{
        HTableInterface table = storage.getTable();
        table.flushCommits();
        table.close();
    }

}
