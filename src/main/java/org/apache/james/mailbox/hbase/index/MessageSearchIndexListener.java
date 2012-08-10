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
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Flags;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

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
            LOG.warn("Problem adding the mail " + message.getUid() + " in mailbox " + message.getMailboxId() + " to the storage!");
        } finally {
            try {
                store.flushToStore();
            } catch (IOException e) {
                //nothing to do
            }
        }
    }

    private List<Put> indexMessage(Message<UUID> message) throws MailboxException {
        List<Put> puts = Lists.newArrayList();
        UUID mailboxId = message.getMailboxId();
        final long messageId = message.getUid();
        for (Map.Entry<MessageFields, String> entry : parseFullContent(message).entries()) {
            Put put = new Put(Bytes.add(uuidToBytes(mailboxId), new byte[]{entry.getKey().id}, Bytes.toBytes(entry.getValue())));
            put.add(HBaseNames.COLUMN_FAMILY.name, Bytes.toBytes(messageId), HBaseNames.EMPTY_COLUMN_VALUE.name);
            puts.add(put);
        }
        return puts;
    }

    public static final byte[] uuidToBytes(UUID uuid) {
        return Bytes.add(Bytes.toBytes(uuid.getMostSignificantBits()),
                Bytes.toBytes(uuid.getLeastSignificantBits()));
    }

    public static final UUID rowToUUID(byte[] row) {
        byte[] uuidz = Bytes.head(row, 16);
        return new UUID(Bytes.toLong(Bytes.head(uuidz, 8)), Bytes.toLong(Bytes.tail(uuidz, 8)));
    }

    public static final MessageFields rowToField(byte[] row) {
        byte[] fieldRead = Bytes.tail(Bytes.head(row, 17), 1);
        for (MessageFields field : MessageFields.values())
            if (field.id == fieldRead[0])
                return field;
        return MessageFields.NOT_FOUND;
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
                    //do nothing
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
            ResultScanner scanner = null;
            try {
                scanner = store.retrieveMails(uuidToBytes(mailbox.getMailboxId()), messageId);
                for (Result result : scanner) {
                    store.updateFlags(result.getRow(), messageId, flags);
                }
            } catch (IOException e) {
                LOG.warn("Couldn't retrieve mail from mailbox");
            } finally {
                try {
                    store.flushToStore();
                    scanner.close();
                } catch (IOException e) {
                    //do nothing
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
            queries.putAll(createQuery(criterion));
        }
        ResultScanner scanner = null;

        try {
            scanner = store.retrieveMails(uuidToBytes(mailbox.getMailboxId()), queries);
            for (Result result : scanner)
                for (byte[] qualifier : result.getFamilyMap(HBaseNames.COLUMN_FAMILY.name).keySet())
                    uids.add(Bytes.toLong(qualifier));
        } catch (IOException e) {
            LOG.warn("Couldn't search through mailbox");
        }
        return uids.iterator();
    }

    /**
     * Return a query which is built based on the given {@link org.apache.james.mailbox.model.SearchQuery.Criterion}
     *
     * @param criterion
     * @return query
     * @throws org.apache.james.mailbox.exception.UnsupportedSearchException
     *
     */
    private Multimap<MessageFields, String> createQuery(SearchQuery.Criterion criterion) throws UnsupportedSearchException, MailboxException {
        if (criterion instanceof SearchQuery.TextCriterion)
            return createTextQuery((SearchQuery.TextCriterion) criterion);
        else if (criterion instanceof SearchQuery.HeaderCriterion)
            return createHeaderQuery((SearchQuery.HeaderCriterion) criterion);

        throw new UnsupportedSearchException();
    }

    private Multimap<MessageFields, String> createTextQuery(SearchQuery.TextCriterion crit) {
        String value = crit.getOperator().getValue().toUpperCase(Locale.ENGLISH);
        Multimap<MessageFields, String> textQuery = ArrayListMultimap.create();
        switch (crit.getType()) {
            case BODY:
                tokenize(MessageFields.BODY_FIELD, value,textQuery);
                break;
            case FULL:
                tokenize(MessageFields.BODY_FIELD, value,textQuery);
                tokenize(MessageFields.HEADERS_FIELD, value,textQuery);
                break;
        }
        return textQuery;
    }

    private Multimap<MessageFields, String> createHeaderQuery(SearchQuery.HeaderCriterion crit) throws UnsupportedSearchException {
        SearchQuery.HeaderOperator op = crit.getOperator();
        String name = crit.getHeaderName().toUpperCase(Locale.ENGLISH);
        Multimap<MessageFields, String> headerQuery = ArrayListMultimap.create();
        String fieldName = MessageFields.PREFIX_HEADER_FIELD + name;
        if (op instanceof SearchQuery.ContainsOperator)
            headerQuery.put(MessageFields.PREFIX_HEADER_FIELD, ((SearchQuery.ContainsOperator) op).getValue().toUpperCase(Locale.ENGLISH));
        else if (op instanceof SearchQuery.ExistsOperator)
            headerQuery.put(MessageFields.PREFIX_HEADER_FIELD, "");
        else /*if (op instanceof SearchQuery.DateOperator) {
            SearchQuery.DateOperator dop = (SearchQuery.DateOperator) op;
            String field = toSentDateField(dop.getDateResultion());
            return createQuery(field, dop);
        } else*/ if (op instanceof SearchQuery.AddressOperator) {
                String address = ((SearchQuery.AddressOperator) op).getAddress().toUpperCase(Locale.ENGLISH);
                tokenize(MessageFields.PREFIX_HEADER_FIELD,address,headerQuery);
            } else // Operator not supported
                throw new UnsupportedSearchException();
        return headerQuery;
    }

    protected static void tokenize(MessageFields field, String value, Multimap<MessageFields, String> map) {
        tokenize(field, new StringReader(value), map);
    }

    protected static void tokenize(MessageFields field, Reader reader, Multimap<MessageFields, String> map) {
        UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer(Version.LUCENE_40, reader);
//                StandardTokenizer tokenizer = new StandardTokenizer(Version.LUCENE_40, reader);
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

    private ArrayListMultimap<MessageFields, String> parseFullContent(final Message<UUID> message) throws MailboxException {
        final ArrayListMultimap<MessageFields, String> map = ArrayListMultimap.create();

        // content handler which will mailbox the headers and the body of the message
        SimpleContentHandler handler = new SimpleContentHandler() {
            public void headers(Header header) {

                Date sentDate = null;
                String firstFromMailbox = "";
                String firstToMailbox = "";
                String firstCcMailbox = "";
                String firstFromDisplay = "";
                String firstToDisplay = "";

                Iterator<org.apache.james.mime4j.stream.Field> fields = header.iterator();
                while (fields.hasNext()) {
                    org.apache.james.mime4j.stream.Field f = fields.next();
                    String headerName = f.getName().toUpperCase(Locale.ENGLISH);
                    String headerValue = f.getBody().toUpperCase(Locale.ENGLISH);
                    String fullValue = f.toString().toUpperCase(Locale.ENGLISH);
                    tokenize(MessageFields.HEADERS_FIELD, fullValue,map);
                    tokenize(MessageFields.PREFIX_HEADER_FIELD, headerValue,map);

                    MessageFields field = null;
                    if ("To".equalsIgnoreCase(headerName)) {
                        field = MessageFields.TO_FIELD;
                    } else if ("From".equalsIgnoreCase(headerName)) {
                        field = MessageFields.FROM_FIELD;
                    } else if ("Cc".equalsIgnoreCase(headerName)) {
                        field = MessageFields.CC_FIELD;
                    } else if ("Bcc".equalsIgnoreCase(headerName)) {
                        field = MessageFields.BCC_FIELD;
                    }


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

                                    if ("To".equalsIgnoreCase(headerName)) {
                                        firstToMailbox = mailboxAddress;
                                        firstToDisplay = mailboxDisplay;
                                    } else if ("From".equalsIgnoreCase(headerName)) {
                                        firstFromMailbox = mailboxAddress;
                                        firstFromDisplay = mailboxDisplay;

                                    } else if ("Cc".equalsIgnoreCase(headerName)) {
                                        firstCcMailbox = mailboxAddress;
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

                                        if ("To".equalsIgnoreCase(headerName)) {
                                            firstToMailbox = mailboxAddress;
                                            firstToDisplay = mailboxDisplay;
                                        } else if ("From".equalsIgnoreCase(headerName)) {
                                            firstFromMailbox = mailboxAddress;
                                            firstFromDisplay = mailboxDisplay;

                                        } else if ("Cc".equalsIgnoreCase(headerName)) {
                                            firstCcMailbox = mailboxAddress;
                                        }
                                    }
                                }
                            }
                        }

                        tokenize(field, headerValue, map);

                    } else if (headerName.equalsIgnoreCase("Subject")) {
                        map.put(MessageFields.BASE_SUBJECT_FIELD, SearchUtil.getBaseSubject(headerValue));
                    }
                }
                if (sentDate == null) {
                    sentDate = message.getInternalDate();
                } else {
                    map.put(MessageFields.SENT_DATE_FIELD, Long.toString(sentDate.getTime()));

                }
                map.put(MessageFields.FIRST_FROM_MAILBOX_NAME_FIELD, firstFromMailbox);
                map.put(MessageFields.FIRST_TO_MAILBOX_NAME_FIELD, firstToMailbox);
                map.put(MessageFields.FIRST_CC_MAILBOX_NAME_FIELD, firstCcMailbox);
                map.put(MessageFields.FIRST_FROM_MAILBOX_DISPLAY_FIELD, firstFromDisplay);
                map.put(MessageFields.FIRST_TO_MAILBOX_DISPLAY_FIELD, firstToDisplay);

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
                    tokenize(MessageFields.BODY_FIELD, new BufferedReader(new InputStreamReader(in, charset)), map);
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
}
