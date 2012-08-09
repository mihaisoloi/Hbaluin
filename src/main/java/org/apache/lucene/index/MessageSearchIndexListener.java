package org.apache.lucene.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.UnsupportedSearchException;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Flags;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.EMPTY_COLUMN_VALUE;
import static org.apache.lucene.index.MessageFields.*;

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
            put.add(COLUMN_FAMILY.name, Bytes.toBytes(messageId), EMPTY_COLUMN_VALUE.name);
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
        // delete a message from index - maybe just mark it in a list and perform the delete on HBase compactions
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
                for (byte[] qualifier : result.getFamilyMap(COLUMN_FAMILY.name).keySet())
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
    private ArrayListMultimap<MessageFields, String> createQuery(SearchQuery.Criterion criterion) throws UnsupportedSearchException, MailboxException {
        if (criterion instanceof SearchQuery.TextCriterion)
            return createTextQuery((SearchQuery.TextCriterion) criterion);
        throw new UnsupportedSearchException();
    }

    public ArrayListMultimap<MessageFields, String> createTextQuery(SearchQuery.TextCriterion crit) {
        String value = crit.getOperator().getValue().toUpperCase(Locale.ENGLISH);
        ArrayListMultimap<MessageFields, String> textQuery = ArrayListMultimap.create();
        switch (crit.getType()) {
            case BODY:
                textQuery.put(BODY_FIELD, value);
                break;
            case FULL:
                textQuery.put(BODY_FIELD, value);
                textQuery.put(HEADERS_FIELD, value);
                break;
        }
        return textQuery;
    }

    private ArrayListMultimap<MessageFields, String> parseFullContent(final Message<UUID> message) throws MailboxException {
        final ArrayListMultimap<MessageFields, String> map = ArrayListMultimap.create();

        // content handler which will index the headers and the body of the message
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
                    map.put(HEADERS_FIELD, fullValue);
                    map.put(PREFIX_HEADER_FIELD, headerValue);

                    MessageFields field = null;
                    if ("To".equalsIgnoreCase(headerName)) {
                        field = TO_FIELD;
                    } else if ("From".equalsIgnoreCase(headerName)) {
                        field = FROM_FIELD;
                    } else if ("Cc".equalsIgnoreCase(headerName)) {
                        field = CC_FIELD;
                    } else if ("Bcc".equalsIgnoreCase(headerName)) {
                        field = BCC_FIELD;
                    }


                    // Check if we can index the the address in the right manner
                    if (field != null) {
                        // not sure if we really should reparse it. It maybe be better to check just for the right type.
                        // But this impl was easier in the first place
                        AddressList aList = LenientAddressBuilder.DEFAULT.parseAddressList(MimeUtil.unfold(f.getBody()));
                        for (int i = 0; i < aList.size(); i++) {
                            Address address = aList.get(i);
                            if (address instanceof org.apache.james.mime4j.dom.address.Mailbox) {
                                org.apache.james.mime4j.dom.address.Mailbox mailbox = (org.apache.james.mime4j.dom.address.Mailbox) address;
                                String value = AddressFormatter.DEFAULT.encode(mailbox).toUpperCase(Locale.ENGLISH);
                                map.put(field, value);
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

                        map.put(field, headerValue);

                    } else if (headerName.equalsIgnoreCase("Subject")) {
                        map.put(BASE_SUBJECT_FIELD, SearchUtil.getBaseSubject(headerValue));
                    }
                }
                if (sentDate == null) {
                    sentDate = message.getInternalDate();
                } else {
                    map.put(SENT_DATE_FIELD, Long.toString(sentDate.getTime()));

                }
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
                    BufferedReader bodyReader = new BufferedReader(new InputStreamReader(in, charset));
                    TokenStream tokens = null;
                    try {
                        tokens = new SimpleAnalyzer(Version.LUCENE_40).tokenStream(BODY_FIELD.name(), bodyReader);
                        Class<CharTermAttribute> attribute = CharTermAttribute.class;
                        tokens.addAttribute(attribute);
                        while (tokens.incrementToken()) {
                            map.put(BODY_FIELD, tokens.getAttribute(attribute).toString().toUpperCase(Locale.ENGLISH));
                        }
                        tokens.end();
                    } finally {
                        tokens.close();
                    }

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
            // parse the message to index headers and body
            parser.parse(message.getFullContent());
        } catch (MimeException e) {
            // This should never happen as it was parsed before too without problems.
            throw new MailboxException("Unable to index content of message", e);
        } catch (IOException e) {
            // This should never happen as it was parsed before too without problems.
            // anyway let us just skip the body and headers in the index
            throw new MailboxException("Unable to index content of message", e);
        }

        return map;
    }
}
