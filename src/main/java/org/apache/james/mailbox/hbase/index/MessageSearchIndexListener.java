package org.apache.james.mailbox.hbase.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.UnsupportedSearchException;
import org.apache.james.mailbox.hbase.store.HBaseIndexStore;
import org.apache.james.mailbox.hbase.store.HBaseNames;
import org.apache.james.mailbox.hbase.store.MessageFields;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.store.mail.MessageMapperFactory;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.search.ListeningMessageSearchIndex;
import org.apache.james.mailbox.store.search.SearchUtil;
import org.apache.james.mime4j.MimeException;
import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.address.Address;
import org.apache.james.mime4j.dom.address.AddressList;
import org.apache.james.mime4j.dom.address.Group;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.field.address.AddressFormatter;
import org.apache.james.mime4j.field.address.LenientAddressBuilder;
import org.apache.james.mime4j.message.SimpleContentHandler;
import org.apache.james.mime4j.parser.MimeStreamParser;
import org.apache.james.mime4j.stream.BodyDescriptor;
import org.apache.james.mime4j.stream.MimeConfig;
import org.apache.james.mime4j.util.MimeUtil;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Flags;
import java.io.*;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;

import static javax.mail.Flags.Flag;
import static javax.mail.Flags.Flag.*;
import static org.apache.james.mailbox.hbase.store.HBaseNames.EMPTY_COLUMN_VALUE;
import static org.apache.james.mailbox.hbase.store.MessageFields.*;

public class MessageSearchIndexListener extends ListeningMessageSearchIndex<UUID> {

    private final static Logger LOG = LoggerFactory.getLogger(MessageSearchIndexListener.class);

    private final static Date MAX_DATE;
    private final static Date MIN_DATE;

    static {
        Calendar cal = Calendar.getInstance();
        cal.set(9999, 11, 31);
        MAX_DATE = cal.getTime();

        cal.set(0000, 0, 1);
        MIN_DATE = cal.getTime();
    }

    private final static String MEDIA_TYPE_TEXT = "text";
    private final static String MEDIA_TYPE_MESSAGE = "message";
    private final static String DEFAULT_ENCODING = "US-ASCII";
    private HBaseIndexStore store;

    public MessageSearchIndexListener(MessageMapperFactory<UUID> factory, HBaseIndexStore store) throws IOException {
        super(factory);
        this.store = store;
    }

    @Override
    public void add(MailboxSession session, Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        try {
            store.storeMail(indexMessage(message));
        } catch (IOException e) {
            throw new MailboxException("Problem adding the mail " + message.getUid() +
                    " in mailbox " + message.getMailboxId() + " to the storage!", e);
        } finally {
            try {
                store.flushToStore();
            } catch (IOException e) {
                LOG.warn("Storing index in table has failed.");
            }
        }
    }

    private List<Put> indexMessage(Message<UUID> message) throws MailboxException {
        final List<Put> puts = Lists.newArrayList();
        final UUID mailboxId = message.getMailboxId();
        final long messageId = message.getUid();
        //add flags
        Put put = new Put(Bytes.add(uuidToBytes(mailboxId), new byte[]{FLAGS_FIELD.id}));
        put.add(HBaseNames.COLUMN_FAMILY.name, Bytes.toBytes(messageId), Bytes.toBytes(parseFlagsContent(message)));
        puts.add(put);
        //add full content
        for (Map.Entry<MessageFields, String> entry : parseFullContent(message).entries()) {
            put = new Put(Bytes.add(uuidToBytes(mailboxId), new byte[]{entry.getKey().id}, Bytes.toBytes(entry.getValue())));
            put.add(HBaseNames.COLUMN_FAMILY.name, Bytes.toBytes(messageId), EMPTY_COLUMN_VALUE.name);
            puts.add(put);
        }
        return puts;
    }

    public static byte[] uuidToBytes(UUID uuid) {
        return Bytes.add(Bytes.toBytes(uuid.getMostSignificantBits()),
                Bytes.toBytes(uuid.getLeastSignificantBits()));
    }

    public static UUID rowToUUID(byte[] row) {
        byte[] uuidz = Bytes.head(row, 16);
        return new UUID(Bytes.toLong(Bytes.head(uuidz, 8)), Bytes.toLong(Bytes.tail(uuidz, 8)));
    }

    public static MessageFields rowToField(byte[] row) {
        byte[] fieldRead = Bytes.tail(Bytes.head(row, 17), 1);
        for (MessageFields field : MessageFields.values())
            if (field.id == fieldRead[0])
                return field;
        return NOT_FOUND;
    }

    public static String rowToTerm(byte[] row) {
        byte[] term = Bytes.tail(row, row.length - 17);
        return Bytes.toString(term);
    }

    @Override
    public void delete(MailboxSession session, Mailbox<UUID> mailbox, MessageRange range) throws MailboxException {
        // delete a message from mailbox - maybe just mark it in a list and perform the delete on HBase compactions
        for (Long messageId : range) {
            ResultScanner scanner = null;
            try {
                scanner = store.retrieveMails(uuidToBytes(mailbox.getMailboxId()), messageId);
                for (Result result : scanner) {
                    store.deleteMail(result.getRow(), messageId);
                }
            } catch (IOException e) {
                LOG.warn("Couldn't delete mail from mailbox");
            } finally {
                try {
                    store.flushToStore();
                    scanner.close();
                } catch (IOException e) {
                    LOG.warn("Storing index in table has failed.");
                }
            }
        }
    }


    /**
     * all previous flags are deleted upon update
     *
     * @param session
     * @param mailbox
     * @param range
     * @param flags
     * @throws MailboxException
     */
    @Override
    public void update(MailboxSession session, Mailbox<UUID> mailbox, MessageRange range, Flags flags) throws MailboxException {
        // update the cells that changed - this means update the flags (and maybe other metadata).
        // message body and headers are immutable so they do not change
        for (Long messageId : range) {
            Result result;
            try {
                result = store.retrieveFlags(uuidToBytes(mailbox.getMailboxId()), messageId);
                store.updateFlags(result.getRow(), messageId, parseFlagsContent(flags));
            } catch (IOException e) {
                throw new MailboxException("Couldn't retrieve flags", e);
            } finally {
                try {
                    store.flushToStore();
                } catch (IOException e) {
                    LOG.warn("Storing index in table has failed.");
                }
            }
        }
    }

    @Override
    public Iterator<Long> search(MailboxSession session, Mailbox<UUID> mailbox, SearchQuery searchQuery) throws MailboxException {
        // return a list of search results
        Set<Long> uids = Sets.newLinkedHashSet();
        ArrayListMultimap<MessageFields, String> queries = ArrayListMultimap.create();
        for (SearchQuery.Criterion criterion : searchQuery.getCriterias()) {
            queries.putAll(createQuery(criterion, mailbox, searchQuery.getRecentMessageUids()));
        }
        ResultScanner scanner = null;

        try {
            scanner = store.retrieveMails(uuidToBytes(mailbox.getMailboxId()), queries);
            for (Result result : scanner)
                for (byte[] qualifier : result.getFamilyMap(HBaseNames.COLUMN_FAMILY.name).keySet())
                    uids.add(Bytes.toLong(qualifier));
        } catch (IOException e) {
            throw new MailboxException("Unable to search mailbox " + mailbox, e);
        }
        return uids.iterator();
    }

    /**
     * Return a query which is built based on the given {@link org.apache.james.mailbox.model.SearchQuery.Criterion}
     */
    private Multimap<MessageFields, String> createQuery(SearchQuery.Criterion criterion, Mailbox<UUID> mailbox, Set<Long> recentUids) throws MailboxException {
        if (criterion instanceof SearchQuery.InternalDateCriterion)
            return createInternalDateQuery((SearchQuery.InternalDateCriterion) criterion);
        else if (criterion instanceof SearchQuery.TextCriterion)
            return createTextQuery((SearchQuery.TextCriterion) criterion);
        else if (criterion instanceof SearchQuery.FlagCriterion) {
            SearchQuery.FlagCriterion crit = (SearchQuery.FlagCriterion) criterion;
            return createFlagQuery(toString(crit.getFlag()), crit.getOperator().isSet(), mailbox, recentUids);
        } else if (criterion instanceof SearchQuery.CustomFlagCriterion) {
            SearchQuery.CustomFlagCriterion crit = (SearchQuery.CustomFlagCriterion) criterion;
            return createFlagQuery(crit.getFlag(), crit.getOperator().isSet(), mailbox, recentUids);
        } else if (criterion instanceof SearchQuery.HeaderCriterion)
            return createHeaderQuery((SearchQuery.HeaderCriterion) criterion);
        else if (criterion instanceof SearchQuery.AllCriterion) //searches on all mail uids on that mailbox
            return ArrayListMultimap.create();

        throw new UnsupportedSearchException();
    }

    private Multimap<MessageFields, String> createInternalDateQuery(SearchQuery.InternalDateCriterion crit) throws UnsupportedSearchException {
        final Multimap<MessageFields, String> dateQuery = ArrayListMultimap.create();
        SearchQuery.DateOperator dop = crit.getOperator();
        Date date = dop.getDate();
        SearchQuery.DateResolution res = dop.getDateResultion();
        DateTools.Resolution dRes = toResolution(res);
        String value = DateTools.dateToString(date, dRes);
        long time = 0l;
        try {
            time = DateTools.stringToTime(value);
        } catch (ParseException e) {
            //do nothing
        }
        System.out.println(value+"~~~~~~~~~"+date.getTime()+"~~~~~~~"+time);
        switch(dop.getType()) {
            case ON:
                dateQuery.put(SENT_DATE_FIELD,"0"+Long.toString(time));
                break;
            case BEFORE:
                dateQuery.put(SENT_DATE_FIELD,"1"+Long.toString(time));
            case AFTER:
                dateQuery.put(SENT_DATE_FIELD,"2"+Long.toString(time));
            default:
                throw new UnsupportedSearchException();
        }
        return dateQuery;
    }

    private DateTools.Resolution toResolution(SearchQuery.DateResolution res) {
        switch (res) {
            case Year:
                return DateTools.Resolution.YEAR;
            case Month:
                return DateTools.Resolution.MONTH;
            case Day:
                return DateTools.Resolution.DAY;
            case Hour:
                return DateTools.Resolution.HOUR;
            case Minute:
                return DateTools.Resolution.MINUTE;
            case Second:
                return DateTools.Resolution.SECOND;
            default:
                return DateTools.Resolution.MILLISECOND;
        }
    }

    private Multimap<MessageFields, String> createFlagQuery(String flag, boolean isSet, Mailbox<UUID> mailbox, Set<Long> recentUids) {
        final Multimap<MessageFields, String> flagsQuery = ArrayListMultimap.create();
        flagsQuery.put(FLAGS_FIELD, isSet ? flag : EMPTY_COLUMN_VALUE.toString());
        return flagsQuery;
    }

    private Multimap<MessageFields, String> createTextQuery(SearchQuery.TextCriterion crit) {
        String value = crit.getOperator().getValue().toUpperCase(Locale.ENGLISH);
        Multimap<MessageFields, String> textQuery = ArrayListMultimap.create();
        switch (crit.getType()) {
            case BODY:
                tokenize(BODY_FIELD, value, textQuery);
                break;
            case FULL:
                tokenize(BODY_FIELD, value, textQuery);
                tokenize(HEADERS_FIELD, value, textQuery);
                break;
        }
        return textQuery;
    }

    private Multimap<MessageFields, String> createHeaderQuery(SearchQuery.HeaderCriterion crit) throws UnsupportedSearchException {
        SearchQuery.HeaderOperator op = crit.getOperator();
        MessageFields field = getHeaderField(crit.getHeaderName());
        Multimap<MessageFields, String> headerQuery = ArrayListMultimap.create();
        if (op instanceof SearchQuery.ContainsOperator) {
            String containedInHeader = ((SearchQuery.ContainsOperator) op).getValue().toUpperCase(Locale.ENGLISH);
            headerQuery.put(field, containedInHeader);
        } else if (op instanceof SearchQuery.ExistsOperator)
            headerQuery.put(field, "");
        else /*if (op instanceof SearchQuery.DateOperator) {
            SearchQuery.DateOperator dop = (SearchQuery.DateOperator) op;
            String field = toSentDateField(dop.getDateResultion());
            return createQuery(field, dop);
        } else*/ if (op instanceof SearchQuery.AddressOperator) {
                String address = ((SearchQuery.AddressOperator) op).getAddress().toUpperCase(Locale.ENGLISH);
                tokenize(field, address, headerQuery);
            } else // Operator not supported
                throw new UnsupportedSearchException();
        return headerQuery;
    }

    private static void tokenize(MessageFields field, String value, Multimap<MessageFields, String> map) {
        tokenize(field, new StringReader(value), map);
    }

    private static void tokenize(MessageFields field, Reader reader, Multimap<MessageFields, String> map) {
        UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer(Version.LUCENE_40, reader);
        tokenizer.addAttribute(CharTermAttribute.class);
        try {
            while (tokenizer.incrementToken())
                map.put(field, tokenizer.getAttribute(CharTermAttribute.class).toString().toUpperCase(Locale.ENGLISH));
        } catch (IOException ioe) {
            LOG.warn("Problem tokenizing " + field.name(), ioe);
        } finally {
            IOUtils.closeQuietly(tokenizer);
        }
    }

    private MessageFields getHeaderField(String headerName) {
        if ("To".equalsIgnoreCase(headerName))
            return TO_FIELD;
        else if ("From".equalsIgnoreCase(headerName))
            return FROM_FIELD;
        else if ("Cc".equalsIgnoreCase(headerName))
            return CC_FIELD;
        else if ("Bcc".equalsIgnoreCase(headerName))
            return BCC_FIELD;
        else if ("Subject".equalsIgnoreCase(headerName))
            return BASE_SUBJECT_FIELD;
        return PREFIX_HEADER_FIELD;
    }

    private ArrayListMultimap<MessageFields, String> parseFullContent(final Message<UUID> message) throws MailboxException {
        final ArrayListMultimap<MessageFields, String> map = ArrayListMultimap.create();

        // content handler which will mailbox the headers and the body of the message
        SimpleContentHandler handler = new SimpleContentHandler() {
            public void headers(Header header) {

                String firstFromMailbox = "";
                String firstToMailbox = "";
                String firstCcMailbox = "";
                String firstFromDisplay = "";
                String firstToDisplay = "";

                for (org.apache.james.mime4j.stream.Field f : header) {
                    String headerName = f.getName().toUpperCase(Locale.ENGLISH);
                    String headerValue = f.getBody().toUpperCase(Locale.ENGLISH);
                    String fullValue = f.toString().toUpperCase(Locale.ENGLISH);
                    tokenize(HEADERS_FIELD, fullValue, map);
                    tokenize(PREFIX_HEADER_FIELD, headerValue, map);

                    MessageFields field = getHeaderField(headerName);

                    // Check if we can mailbox the the address in the right manner
                    if (field != null) {
                        // not sure if we really should reparse it. It maybe be better to check just for the right type.
                        // But this impl was easier in the first place
                        AddressList aList = LenientAddressBuilder.DEFAULT.parseAddressList(MimeUtil.unfold(f.getBody()));
                        for (int i = 0; i < aList.size(); i++) {
                            Address address = aList.get(i);
                            if (address instanceof org.apache.james.mime4j.dom.address.Mailbox) {
                                org.apache.james.mime4j.dom.address.Mailbox mailbox = (org.apache.james.mime4j.dom.address.Mailbox) address;
                                String value = AddressFormatter.DEFAULT.encode(mailbox).toUpperCase(Locale.ENGLISH);
                                tokenize(field, value, map);
                                if (i == 0) {
                                    String mailboxAddress = SearchUtil.getMailboxAddress(mailbox);
                                    String mailboxDisplay = SearchUtil.getDisplayAddress(mailbox);

                                    switch (field) {
                                        case TO_FIELD:
                                            firstToMailbox = mailboxAddress;
                                            firstToDisplay = mailboxDisplay;
                                            break;
                                        case FROM_FIELD:
                                            firstFromMailbox = mailboxAddress;
                                            firstFromDisplay = mailboxDisplay;
                                            break;
                                        case CC_FIELD:
                                            firstCcMailbox = mailboxAddress;
                                            break;
                                    }
                                }
                            } else if (address instanceof Group) {
                                MailboxList mList = ((Group) address).getMailboxes();
                                for (int a = 0; a < mList.size(); a++) {
                                    org.apache.james.mime4j.dom.address.Mailbox mailbox = mList.get(a);
                                    String value = AddressFormatter.DEFAULT.encode(mailbox).toUpperCase(Locale.ENGLISH);
                                    map.put(field, value);

                                    if (i == 0 && a == 0) {
                                        String mailboxAddress = SearchUtil.getMailboxAddress(mailbox);
                                        String mailboxDisplay = SearchUtil.getDisplayAddress(mailbox);

                                        switch (field) {
                                            case TO_FIELD:
                                                firstToMailbox = mailboxAddress;
                                                firstToDisplay = mailboxDisplay;
                                                break;
                                            case FROM_FIELD:
                                                firstFromMailbox = mailboxAddress;
                                                firstFromDisplay = mailboxDisplay;
                                                break;
                                            case CC_FIELD:
                                                firstCcMailbox = mailboxAddress;
                                                break;
                                        }
                                    }
                                }
                            }
                        }

                        tokenize(field, headerValue, map);

                    } else if (headerName.equalsIgnoreCase("Subject")) {
                        map.put(BASE_SUBJECT_FIELD, SearchUtil.getBaseSubject(headerValue));
                    }
                }

                map.put(SENT_DATE_FIELD, Long.toString(message.getInternalDate().getTime()));
                map.put(FIRST_FROM_MAILBOX_NAME_FIELD, firstFromMailbox);
                map.put(FIRST_TO_MAILBOX_NAME_FIELD, firstToMailbox);
                map.put(FIRST_CC_MAILBOX_NAME_FIELD, firstCcMailbox);
                map.put(FIRST_FROM_MAILBOX_DISPLAY_FIELD, firstFromDisplay);
                map.put(FIRST_TO_MAILBOX_DISPLAY_FIELD, firstToDisplay);

            }

            @Override
            public void body(BodyDescriptor desc, InputStream in) throws MimeException, IOException {
                String mediaType = desc.getMediaType();
                if (MEDIA_TYPE_TEXT.equalsIgnoreCase(mediaType) || MEDIA_TYPE_MESSAGE.equalsIgnoreCase(mediaType)) {
                    String cset = desc.getCharset();
                    if (cset == null) {
                        cset = DEFAULT_ENCODING;
                    }
                    Charset charset;
                    try {
                        charset = Charset.forName(cset);
                    } catch (Exception e) {
                        // Invalid charset found so fallback toe the DEFAULT_ENCODING
                        charset = Charset.forName(DEFAULT_ENCODING);
                    }

                    // Read the content one line after the other and add it to the document
                    tokenize(BODY_FIELD, new BufferedReader(new InputStreamReader(in, charset)), map);
                }
            }

        };
        MimeConfig config = new MimeConfig();
        config.setMaxLineLen(-1);
        //config.setStrictParsing(false);
        config.setMaxContentLen(-1);
        MimeStreamParser parser = new MimeStreamParser(config);
        parser.setContentDecoding(true);
        parser.setContentHandler(handler);

        try {
            // parse the message to mailbox headers and body
            parser.parse(message.getFullContent());
        } catch (MimeException e) {
            // This should never happen as it was parsed before too without problems.
            throw new MailboxException("Unable to mailbox content of message", e);
        } catch (IOException e) {
            // This should never happen as it was parsed before too without problems.
            // anyway let us just skip the body and headers in the mailbox
            throw new MailboxException("Unable to mailbox content of message", e);
        }

        return map;
    }

    private String parseFlagsContent(Message<?> message) {
        return parseFlagsContent(message.createFlags());
    }

    private String parseFlagsContent(Flags flags) {
        final StringBuilder sb = new StringBuilder();
        Flag[] systemFlags = flags.getSystemFlags();
        String[] userFlags = flags.getUserFlags();

        if (systemFlags.length == 0 && userFlags.length == 0)
            sb.append(EMPTY_COLUMN_VALUE.toString());
        else {
            for (Flag systemFlag : systemFlags)
                sb.append(toString(systemFlag));

            for (String userFlag : userFlags)
                sb.append(userFlag);
        }
        return sb.toString();
    }

    /**
     * Convert the given {@link Flag} to a String
     *
     * @param flag
     * @return flagString
     */
    private String toString(Flag flag) {
        if (ANSWERED.equals(flag)) {
            return "\\ANSWERED";
        } else if (DELETED.equals(flag)) {
            return "\\DELETED";
        } else if (DRAFT.equals(flag)) {
            return "\\DRAFT";
        } else if (FLAGGED.equals(flag)) {
            return "\\FLAGGED";
        } else if (RECENT.equals(flag)) {
            return "\\RECENT";
        } else if (SEEN.equals(flag)) {
            return "\\FLAG";
        } else {
            return flag.toString();
        }
    }
}
