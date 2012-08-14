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

import java.io.IOException;
import java.sql.Time;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.james.mailbox.hbase.store.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.hbase.store.MessageFields.FLAGS_FIELD;

public class HBaseIndexStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexStore.class);
    private static HBaseIndexStore store;
    private static HTableInterface table;

    private HBaseIndexStore() {
    }

    public static synchronized HBaseIndexStore getInstance(final Configuration configuration) throws IOException {
        if (store == null) {
            store = new HBaseIndexStore();
            HBaseAdmin admin = new HBaseAdmin(configuration);

            HTableDescriptor htd = new HTableDescriptor(HBaseNames.INDEX_TABLE.name);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(COLUMN_FAMILY.name);
            htd.addFamily(columnDescriptor);
            admin.createTable(htd);
            table = new HTable(configuration, HBaseNames.INDEX_TABLE.name);
        }
        return store;
    }

    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
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
        scan.addFamily(COLUMN_FAMILY.name);
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public ResultScanner retrieveMails(byte[] mailboxId, long messageId) throws IOException {
        if (messageId == 0l)
            return retrieveMails(mailboxId);
        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public ResultScanner retrieveMails(byte[] mailboxId, ArrayListMultimap<MessageFields, String> queries) throws IOException {
        if (queries.isEmpty())
            return retrieveMails(mailboxId);
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Map.Entry<MessageFields, String> query : queries.entries()) {
            String term = query.getValue().toUpperCase(Locale.ENGLISH);
            byte[] field = new byte[]{query.getKey().id};
            byte[] prefix = Bytes.add(mailboxId, field);
            switch(query.getKey()){
                case FLAGS_FIELD:
                    final FilterList flagList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new BinaryComparator(prefix));
                    ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(term));
                    flagList.addFilter(rowFilter);
                    flagList.addFilter(valueFilter);
                    list.addFilter(flagList);
                    break;
                case SENT_DATE_FIELD:
                    long time = Long.parseLong(term.substring(1));
                    long day = 24 * 60 * 60 * 1000;
                    long max = time + day;
                    long now = new Date().getTime();
                    assert(now > time);
                    assert(now < max);
                    System.out.println((Bytes.compareTo(Bytes.add(prefix,Bytes.toBytes(now)),Bytes.add(prefix,Bytes.toBytes(time)))>0));
                    switch(term.charAt(0)){
                        case 0://ON
                            FilterList onTime = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                            RowFilter rowFilter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                                    new BinaryComparator(Bytes.add(prefix,Bytes.toBytes(time))));
                            RowFilter rowFilter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                                    new BinaryComparator(Bytes.add(prefix,Bytes.toBytes(max))));
                            onTime.addFilter(rowFilter1);
                            onTime.addFilter(rowFilter2);
                            list.addFilter(onTime);
                            break;
                        case 1://BEFORE
                            break;
                        case 2://AFTER
                            break;
                    }
                    break;
                default:
                    RowFilter rowFilterPrefix = new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new BinaryPrefixComparator(Bytes.add(prefix, Bytes.toBytes(term))));
                    RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new RegexStringComparator(Bytes.toString(prefix) + ".*?" + term + ".*+"));
                    list.addFilter(rowFilterPrefix);
                    list.addFilter(rowFilterRegex);
                    break;
            }
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

    public Result retrieveFlags(byte[] mailboxId, long messageId) throws IOException {
        Get get = new Get(Bytes.add(mailboxId, new byte[]{FLAGS_FIELD.id}));
        get.addColumn(COLUMN_FAMILY.name, Bytes.toBytes(messageId));
        return table.get(get);
    }

    public void updateFlags(byte[] row, long messageId, String flags) throws IOException {
        Put put = new Put(row);
        put.add(COLUMN_FAMILY.name, Bytes.toBytes(messageId), Bytes.toBytes(flags));
        table.put(put);
    }


}
