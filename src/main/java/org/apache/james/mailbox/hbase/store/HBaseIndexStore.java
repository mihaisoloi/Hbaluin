package org.apache.james.mailbox.hbase.store;

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

        HTableDescriptor htd = new HTableDescriptor(HBaseNames.INDEX_TABLE.name);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(HBaseNames.COLUMN_FAMILY.name);
        htd.addFamily(columnDescriptor);
        admin.createTable(htd);
        table = new HTable(configuration, HBaseNames.INDEX_TABLE.name);
        return table;
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

    public ResultScanner retrieveMails(byte[] mailboxId) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(HBaseNames.COLUMN_FAMILY.name);
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public ResultScanner retrieveMails(byte[] mailboxId, long messageId) throws IOException {
        if (messageId == 0l)
            return retrieveMails(mailboxId);
        Scan scan = new Scan();
        scan.addColumn(HBaseNames.COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public ResultScanner retrieveMails(byte[] mailboxId, ArrayListMultimap<MessageFields, String> queries) throws IOException {
        if (queries.isEmpty())
            return retrieveMails(mailboxId);
        Scan scan = new Scan();
        scan.addFamily(HBaseNames.COLUMN_FAMILY.name);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Map.Entry<MessageFields, String> query : queries.entries()) {
            String term = query.getValue().toUpperCase(Locale.ENGLISH);
            byte[] field = new byte[]{query.getKey().id};
            byte[] prefix = Bytes.add(mailboxId, field);
            RowFilter rowFilterPrefix = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.add(prefix, Bytes.toBytes(term))));
            RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(Bytes.toString(prefix) + ".*?" + term + ".*+"));
            list.addFilter(rowFilterPrefix);
            list.addFilter(rowFilterRegex);
        }
        scan.setFilter(list);
        return table.getScanner(scan);
    }

    public void deleteMail(byte[] row, long messageId) throws IOException {
        Delete delete = new Delete(row);
        delete.deleteColumn(HBaseNames.COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        table.delete(delete);
    }

    public void flushToStore() throws IOException {
        table.flushCommits();
    }

    public void updateFlags(byte[] row, Long messageId, Flags flags) {
        Scan scan = new Scan();
    }


}