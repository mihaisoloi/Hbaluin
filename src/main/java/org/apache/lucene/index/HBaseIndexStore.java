package org.apache.lucene.index;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Flags;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.INDEX_TABLE;

public class HBaseIndexStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexStore.class);

    private static HTableInterface table;

    public HBaseIndexStore() {
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
        table = new HTable(configuration, INDEX_TABLE.name);
        return table;
    }

    public Put persistTerm(String mailboxId, int docId, String field, byte[] term) throws IOException {
        //row = mailboxID - term
        Put put = new Put(Bytes.add(Bytes.toBytes(mailboxId), Constants.SEPARATOR, term));
        // family=column_family, qualifier = documentID,value = fields
        put.add(COLUMN_FAMILY.name, Bytes.toBytes(docId), Bytes.toBytes(field));
        return put;
    }

    /**
     * writes the rows as puts in HBase where the qualifier is composed of the mailID
     *
     * @param puts
     * @throws IOException
     */
    public void storeMail(List<Put> puts) throws IOException {
        for (Put put : puts) {
            table.put(put);
        }
    }

    public ResultScanner retrieveMails(byte[] mailboxId, long messageId) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public ResultScanner retrieveMails(byte[] mailboxId, ArrayListMultimap<MessageFields, String> queries) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Map.Entry<MessageFields, String> query : queries.entries()) {
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.add(mailboxId,
                            new byte[]{query.getKey().id},
                            Bytes.toBytes(query.getValue().toUpperCase(Locale.ENGLISH)))));
            list.addFilter(rowFilter);
        }
        scan.setFilter(list);
        return table.getScanner(scan);
    }

    public void deleteMail(byte[] row, long messageId) throws IOException {
        Delete delete = new Delete(row);
        delete.deleteColumn(COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        table.delete(delete);
    }

    public void flushToStore() throws IOException {
        table.flushCommits();
    }

    public void updateFlags(byte[] row, Long messageId, Flags flags) {
        Scan scan = new Scan();
    }


}
