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
import org.apache.lucene.document.DateTools;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.james.mailbox.hbase.index.MessageSearchIndexListener.row;
import static org.apache.james.mailbox.hbase.index.MessageSearchIndexListener.rowToField;
import static org.apache.james.mailbox.hbase.index.MessageSearchIndexListener.rowToTerm;
import static org.apache.james.mailbox.hbase.store.HBaseNames.COLUMN_FAMILY;

public class RowFilteringEndpoint extends BaseEndpointCoprocessor implements RowFilteringProtocol {

    private final static Date MAX_DATE;
    private final static Date MIN_DATE;

    static {
        Calendar cal = Calendar.getInstance();
        cal.set(9999, 11, 31);
        MAX_DATE = cal.getTime();

        cal.set(0000, 0, 1);
        MIN_DATE = cal.getTime();
    }

    @Override
    public Set<Long> filterByQueries(byte[] mailboxId, ArrayListMultimap<MessageFields, String> queries) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY.name);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Map.Entry<MessageFields, String> query : queries.entries()) {
            String term = query.getValue().toUpperCase(Locale.ENGLISH);
            byte[] field = new byte[]{query.getKey().id};
            byte[] prefix = Bytes.add(mailboxId, field);
            switch (query.getKey()) {
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
                    int separatorIndex = term.indexOf("|");
                    long time = Long.parseLong(term.substring(separatorIndex + 1));
                    long max = getMaxResolution(term.substring(1, separatorIndex), time);

                    FilterList timeList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    long lowerBound = 0, upperBound = 0;
                    switch (term.charAt(0)) {
                        case '0'://ON
                            lowerBound = time;
                            upperBound = max;
                            break;
                        case '1'://BEFORE
                            lowerBound = MIN_DATE.getTime();
                            upperBound = time;
                            break;
                        case '2'://AFTER
                            lowerBound = max;
                            upperBound = Long.MAX_VALUE;
                            break;
                    }
                    timeList.addFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                            new BinaryComparator(Bytes.add(prefix, Bytes.toBytes(Long.toString(lowerBound))))));
                    timeList.addFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                            new BinaryComparator(Bytes.add(prefix, Bytes.toBytes(Long.toString(upperBound))))));
                    list.addFilter(timeList);
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

        return extractIds(scan);
    }

    static class BinaryComparator extends org.apache.hadoop.hbase.filter.BinaryComparator {

        public BinaryComparator() {
        }

        /**
         * Constructor
         *
         * @param value value
         */
        public BinaryComparator(byte[] value) {
            super(value);
        }

        @Override
        public int compareTo(byte[] value, int offset, int length) {
            int compare = Bytes.compareTo(this.getValue(), 0, this.getValue().length, value, offset, value.length);
            if (rowToField(value) == MessageFields.SENT_DATE_FIELD
                    && Long.parseLong(rowToTerm(this.getValue())) > Long.parseLong(rowToTerm(value))
                    && compare<0) {
                System.out.printf("%s %s %s : %s %n %s %b %n",
                        DateTools.timeToString(Long.parseLong(rowToTerm(this.getValue())), DateTools.Resolution.MILLISECOND),
                        compare > 0 ? ">" : "<",
                        DateTools.timeToString(Long.parseLong(rowToTerm(value)), DateTools.Resolution.MILLISECOND),
                        offset + "-" + length,
                        rowToTerm(this.getValue()) + "~~~" + rowToTerm(value),
                        Long.parseLong(rowToTerm(this.getValue())) > Long.parseLong(rowToTerm(value)));
                System.out.println(row(this.getValue()));
                System.out.println(row(value));
            }
            return compare;
        }
    }

    public long getMaxResolution(String name, long time) {
        long diff = 1l;
        final Calendar max = Calendar.getInstance();
        max.setTimeInMillis(time);
        switch (DateTools.Resolution.valueOf(name)) {
            case YEAR:
                max.set(Calendar.YEAR, max.get(Calendar.YEAR) + 1);
                return max.getTimeInMillis();
            case MONTH:
                max.set(Calendar.MONTH, max.get(Calendar.MONTH) + 1);
                return max.getTimeInMillis();
            case DAY:
                return time + TimeUnit.DAYS.toMillis(diff);
            case HOUR:
                return time + TimeUnit.HOURS.toMillis(diff);
            case MINUTE:
                return time + TimeUnit.MINUTES.toMillis(diff);
            case SECOND:
                return time + TimeUnit.SECONDS.toMillis(diff);
            default:
                return time;
        }
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