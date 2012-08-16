package org.apache.james.mailbox.hbase.store.endpoint;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.hbase.store.HBaseNames;
import org.apache.james.mailbox.hbase.store.MessageFields;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.james.mailbox.hbase.store.HBaseNames.COLUMN_FAMILY;

public class RowFilteringEndpoint extends BaseEndpointCoprocessor implements RowFilteringProtocol {

    @Override
    public Set<Long> filterByQueries(byte[] mailboxId, ArrayListMultimap<MessageFields, String> queries) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Map.Entry<MessageFields, String> query : queries.entries()) {
            String term = query.getValue().toUpperCase(Locale.ENGLISH);
            byte[] field = new byte[]{query.getKey().id};
            byte[] prefix = Bytes.add(mailboxId, field);
            if (query.getKey() == MessageFields.FLAGS_FIELD) {
                final FilterList flagList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(prefix));
                ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                        new SubstringComparator(term));
                flagList.addFilter(rowFilter);
                flagList.addFilter(valueFilter);
                list.addFilter(flagList);
            } else {
                RowFilter rowFilterPrefix = new RowFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryPrefixComparator(Bytes.add(prefix, Bytes.toBytes(term))));
                RowFilter rowFilterRegex = new RowFilter(CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(Bytes.toString(prefix) + ".*?" + term + ".*+"));
                list.addFilter(rowFilterPrefix);
                list.addFilter(rowFilterRegex);
            }
        }
        scan.setFilter(list);

        return extractIds(scan);
    }

    @Override
    public Set<Long> filterByMailbox(byte[] mailboxId) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(mailboxId));
        scan.setFilter(filter);
        return extractIds(scan);
    }

    private Set<Long> extractIds(Scan scan) throws IOException {
        Set<Long> uids = Sets.newLinkedHashSet();
        ResultScanner scanner = getEnvironment().getTable(HBaseNames.INDEX_TABLE.name).getScanner(scan);
        for (Result result : scanner)
            for (byte[] qualifier : result.getFamilyMap(HBaseNames.COLUMN_FAMILY.name).keySet())
                uids.add(Bytes.toLong(qualifier));
        return uids;
    }
}
