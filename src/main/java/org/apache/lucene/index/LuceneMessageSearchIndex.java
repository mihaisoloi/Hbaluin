/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.lucene.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.lucene.search.LenientImapSearchAnalyzer;
import org.apache.james.mailbox.lucene.search.StrictImapSearchAnalyzer;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.model.SearchQuery.*;
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
import org.apache.james.mime4j.dom.datetime.DateTime;
import org.apache.james.mime4j.dom.field.DateTimeField;
import org.apache.james.mime4j.field.address.AddressFormatter;
import org.apache.james.mime4j.field.address.LenientAddressBuilder;
import org.apache.james.mime4j.field.datetime.parser.DateTimeParser;
import org.apache.james.mime4j.message.SimpleContentHandler;
import org.apache.james.mime4j.parser.MimeStreamParser;
import org.apache.james.mime4j.stream.BodyDescriptor;
import org.apache.james.mime4j.stream.MimeConfig;
import org.apache.james.mime4j.util.MimeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.*;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import javax.mail.Flags;
import javax.mail.Flags.Flag;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.INDEX_TABLE;

/**
 * Lucene based {@link ListeningMessageSearchIndex} which offers message searching via a Lucene index
 *
 * @param <Id>
 */
public class LuceneMessageSearchIndex<Id> extends ListeningMessageSearchIndex<Id> {
    private final static Date MAX_DATE;
    private final static Date MIN_DATE;

    static {
        Calendar cal = Calendar.getInstance();
        cal.set(9999, 11, 31);
        MAX_DATE = cal.getTime();

        cal.set(0000, 0, 1);
        MIN_DATE = cal.getTime();
    }

    /**
     * Default max query results
     */
    public final static int DEFAULT_MAX_QUERY_RESULTS = 100000;

    /**
     * {@link Field} which will contain the unique index of the {@link Document}
     */
    public final static String ID_FIELD = "id";


    /**
     * {@link Field} which will contain uid of the {@link Message}
     */
    public final static String UID_FIELD = "uid";

    /**
     * {@link Field} which will contain the {@link Flags} of the {@link Message}
     */
    public final static String FLAGS_FIELD = "flags";

    /**
     * {@link Field} which will contain the size of the {@link Message}
     */
    public final static String SIZE_FIELD = "size";

    /**
     * {@link Field} which will contain the body of the {@link Message}
     */
    public final static String BODY_FIELD = "body";


    /**
     * Prefix which will be used for each message header to store it also in a seperate {@link Field}
     */
    public final static String PREFIX_HEADER_FIELD = "header_";

    /**
     * {@link Field} which will contain the whole message header of the {@link Message}
     */
    public final static String HEADERS_FIELD = "headers";

    /**
     * {@link Field} which will contain the mod-sequence of the message
     */
    public final static String MODSEQ_FIELD = "modSeq";

    /**
     * {@link Field} which will contain the TO-Address of the message
     */
    public final static String TO_FIELD = "to";

    public final static String FIRST_TO_MAILBOX_NAME_FIELD = "firstToMailboxName";
    public final static String FIRST_TO_MAILBOX_DISPLAY_FIELD = "firstToMailboxDisplay";

    /**
     * {@link Field} which will contain the CC-Address of the message
     */
    public final static String CC_FIELD = "cc";

    public final static String FIRST_CC_MAILBOX_NAME_FIELD = "firstCcMailboxName";


    /**
     * {@link Field} which will contain the FROM-Address of the message
     */
    public final static String FROM_FIELD = "from";

    public final static String FIRST_FROM_MAILBOX_NAME_FIELD = "firstFromMailboxName";
    public final static String FIRST_FROM_MAILBOX_DISPLAY_FIELD = "firstFromMailboxDisplay";

    /**
     * {@link Field} which will contain the BCC-Address of the message
     */
    public final static String BCC_FIELD = "bcc";


    public final static String BASE_SUBJECT_FIELD = "baseSubject";

    /**
     * {@link Field} which contain the internalDate of the message with YEAR-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_YEAR_RESOLUTION = "internaldateYearResolution";


    /**
     * {@link Field} which contain the internalDate of the message with MONTH-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_MONTH_RESOLUTION = "internaldateMonthResolution";

    /**
     * {@link Field} which contain the internalDate of the message with DAY-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_DAY_RESOLUTION = "internaldateDayResolution";

    /**
     * {@link Field} which contain the internalDate of the message with HOUR-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_HOUR_RESOLUTION = "internaldateHourResolution";

    /**
     * {@link Field} which contain the internalDate of the message with MINUTE-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_MINUTE_RESOLUTION = "internaldateMinuteResolution";

    /**
     * {@link Field} which contain the internalDate of the message with SECOND-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_SECOND_RESOLUTION = "internaldateSecondResolution";


    /**
     * {@link Field} which contain the internalDate of the message with MILLISECOND-Resolution
     */
    public final static String INTERNAL_DATE_FIELD_MILLISECOND_RESOLUTION = "internaldateMillisecondResolution";

    /**
     * {@link Field} which will contain the id of the {@link Mailbox}
     */
    public final static String MAILBOX_ID_FIELD = "mailboxid";

    /**
     * {@link Field} which contain the Date header of the message with YEAR-Resolution
     */
    public final static String SENT_DATE_FIELD_YEAR_RESOLUTION = "sentdateYearResolution";


    /**
     * {@link Field} which contain the Date header of the message with MONTH-Resolution
     */
    public final static String SENT_DATE_FIELD_MONTH_RESOLUTION = "sentdateMonthResolution";

    /**
     * {@link Field} which contain the Date header of the message with DAY-Resolution
     */
    public final static String SENT_DATE_FIELD_DAY_RESOLUTION = "sentdateDayResolution";

    /**
     * {@link Field} which contain the Date header of the message with HOUR-Resolution
     */
    public final static String SENT_DATE_FIELD_HOUR_RESOLUTION = "sentdateHourResolution";

    /**
     * {@link Field} which contain the Date header of the message with MINUTE-Resolution
     */
    public final static String SENT_DATE_FIELD_MINUTE_RESOLUTION = "sentdateMinuteResolution";

    /**
     * {@link Field} which contain the Date header of the message with SECOND-Resolution
     */
    public final static String SENT_DATE_FIELD_SECOND_RESOLUTION = "sentdateSecondResolution";


    /**
     * {@link Field} which contain the Date header of the message with MILLISECOND-Resolution
     */
    public final static String SENT_DATE_FIELD_MILLISECOND_RESOLUTION = "sentdateMillisecondResolution";

    public final static String SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION = "sentdateSort";

    public final static String NON_EXIST_FIELD = "nonExistField";


    private final static String MEDIA_TYPE_TEXT = "text";
    private final static String MEDIA_TYPE_MESSAGE = "message";
    private final static String DEFAULT_ENCODING = "US-ASCII";

    private final MemoryIndexWriter writer;
    private final MemoryIndexReader reader;

    private int maxQueryResults = DEFAULT_MAX_QUERY_RESULTS;

    private boolean suffixMatch = false;

    private final static SortField UID_SORT = new SortField(UID_FIELD, SortField.Type.LONG);
    private final static SortField UID_SORT_REVERSE = new SortField(UID_FIELD, SortField.Type.LONG, true);

    private final static SortField SIZE_SORT = new SortField(SIZE_FIELD, SortField.Type.LONG);
    private final static SortField SIZE_SORT_REVERSE = new SortField(SIZE_FIELD, SortField.Type.LONG, true);

    private final static SortField FIRST_CC_MAILBOX_SORT = new SortField(FIRST_CC_MAILBOX_NAME_FIELD, SortField.Type.STRING);
    private final static SortField FIRST_CC_MAILBOX_SORT_REVERSE = new SortField(FIRST_CC_MAILBOX_NAME_FIELD, SortField.Type.STRING, true);

    private final static SortField FIRST_TO_MAILBOX_SORT = new SortField(FIRST_TO_MAILBOX_NAME_FIELD, SortField.Type.STRING);
    private final static SortField FIRST_TO_MAILBOX_SORT_REVERSE = new SortField(FIRST_TO_MAILBOX_NAME_FIELD, SortField.Type.STRING, true);

    private final static SortField FIRST_FROM_MAILBOX_SORT = new SortField(FIRST_FROM_MAILBOX_NAME_FIELD, SortField.Type.STRING);
    private final static SortField FIRST_FROM_MAILBOX_SORT_REVERSE = new SortField(FIRST_FROM_MAILBOX_NAME_FIELD, SortField.Type.STRING, true);


    private final static SortField ARRIVAL_MAILBOX_SORT = new SortField(INTERNAL_DATE_FIELD_MILLISECOND_RESOLUTION, SortField.Type.LONG);
    private final static SortField ARRIVAL_MAILBOX_SORT_REVERSE = new SortField(INTERNAL_DATE_FIELD_MILLISECOND_RESOLUTION, SortField.Type.LONG, true);

    private final static SortField BASE_SUBJECT_SORT = new SortField(BASE_SUBJECT_FIELD, SortField.Type.STRING);
    private final static SortField BASE_SUBJECT_SORT_REVERSE = new SortField(BASE_SUBJECT_FIELD, SortField.Type.STRING, true);

    private final static SortField SENT_DATE_SORT = new SortField(SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION, SortField.Type.LONG);
    private final static SortField SENT_DATE_SORT_REVERSE = new SortField(SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION, SortField.Type.LONG, true);

    private final static SortField FIRST_TO_MAILBOX_DISPLAY_SORT = new SortField(FIRST_TO_MAILBOX_DISPLAY_FIELD, SortField.Type.STRING);
    private final static SortField FIRST_TO_MAILBOX_DISPLAY_SORT_REVERSE = new SortField(FIRST_TO_MAILBOX_DISPLAY_FIELD, SortField.Type.STRING, true);

    private final static SortField FIRST_FROM_MAILBOX_DISPLAY_SORT = new SortField(FIRST_FROM_MAILBOX_DISPLAY_FIELD, SortField.Type.STRING);
    private final static SortField FIRST_FROM_MAILBOX_DISPLAY_SORT_REVERSE = new SortField(FIRST_FROM_MAILBOX_DISPLAY_FIELD, SortField.Type.STRING, true);


    public LuceneMessageSearchIndex(MessageMapperFactory<Id> factory, FileSystem fileSystem, HBaseAdmin admin) throws CorruptIndexException, LockObtainFailedException, IOException {
        this(factory, fileSystem, admin, new HTablePool(fileSystem.getConf(), 2), false, true);
    }


    public LuceneMessageSearchIndex(MessageMapperFactory<Id> factory, FileSystem fileSystem, HBaseAdmin admin, HTablePool hTablePool, boolean dropIndexOnStart, boolean lenient) throws CorruptIndexException, LockObtainFailedException, IOException {
        super(factory);
        HBaseIndexStore store = new HBaseIndexStore(hTablePool, fileSystem.getConf(), INDEX_TABLE.toString());
        this.writer = new MemoryIndexWriter(store, MAILBOX_ID_FIELD + "-" + UID_FIELD);
        this.reader = new MemoryIndexReader(store, INDEX_TABLE.toString());
    }


    public LuceneMessageSearchIndex(MessageMapperFactory<Id> factory, MemoryIndexWriter writer, MemoryIndexReader reader) {
        super(factory);
        this.writer = writer;
        this.reader = reader;
    }

    /**
     * Set the max count of results which will get returned from a query. The default is {@link #DEFAULT_MAX_QUERY_RESULTS}
     *
     * @param maxQueryResults
     */
    public void setMaxQueryResults(int maxQueryResults) {
        this.maxQueryResults = maxQueryResults;
    }

    protected IndexWriterConfig createConfig(Analyzer analyzer, boolean dropIndexOnStart) {
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        if (dropIndexOnStart) {
            config.setOpenMode(OpenMode.CREATE);
        } else {
            config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }
        return config;
    }

    /**
     * Create a {@link Analyzer} which is used to index the {@link Message}'s
     *
     * @param lenient
     * @return analyzer
     */
    protected Analyzer createAnalyzer(boolean lenient) {
        if (lenient) {
            return new LenientImapSearchAnalyzer();
        } else {
            return new StrictImapSearchAnalyzer();
        }
    }

    /**
     * If set to true this implementation will use {@link WildcardQuery} to match suffix and prefix. This is what RFC3501 expects but is often not what the user does.
     * It also slow things a lot if you have complex queries which use many "TEXT" arguments. If you want the implementation to behave strict like RFC3501 says, you should
     * set this to true.
     * <p/>
     * The default is false for performance reasons
     *
     * @param suffixMatch
     */
    public void setEnableSuffixMatch(boolean suffixMatch) {
        this.suffixMatch = suffixMatch;
    }


    /**
     * @see org.apache.james.mailbox.store.search.MessageSearchIndex#search(org.apache.james.mailbox.MailboxSession, org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.model.SearchQuery)
     */
    public Iterator<Long> search(MailboxSession session, Mailbox<Id> mailbox, SearchQuery searchQuery) throws MailboxException {
        Set<Long> uids = new LinkedHashSet<Long>();
        IndexSearcher searcher = null;

        try {
            searcher = new IndexSearcher(reader);
            BooleanQuery query = new BooleanQuery();
            query.add(new TermQuery(new Term(MAILBOX_ID_FIELD, mailbox.getMailboxId().toString())), BooleanClause.Occur.MUST);
            // Not return flags documents
            query.add(new PrefixQuery(new Term(FLAGS_FIELD, "")), BooleanClause.Occur.MUST_NOT);
            List<Criterion> crits = searchQuery.getCriterias();
            for (int i = 0; i < crits.size(); i++) {
                query.add(createQuery(crits.get(i), mailbox, searchQuery.getRecentMessageUids()), BooleanClause.Occur.MUST);
            }

            // query for all the documents sorted as specified in the SearchQuery
            TopDocs docs = searcher.search(query, null, maxQueryResults, createSort(searchQuery.getSorts()));
            ScoreDoc[] sDocs = docs.scoreDocs;
            for (int i = 0; i < sDocs.length; i++) {
                long uid = Long.valueOf(searcher.doc(sDocs[i].doc).get(UID_FIELD));
                uids.add(uid);
            }
        } catch (IOException e) {
            throw new MailboxException("Unable to search the mailbox", e);
        }
        return uids.iterator();
    }


    /**
     * Create a new {@link Document} for the given {@link Message}. This Document does not contain any flags data. The {@link Flags} are stored in a seperate Document.
     * <p/>
     * See {@link #createFlagsDocument(Message)}
     *
     * @param membership
     * @return document
     */
    private Document createMessageDocument(final MailboxSession session, final Message<?> membership) throws MailboxException {
        final Document doc = new Document();
        // TODO: Better handling
        doc.add(new StringField(MAILBOX_ID_FIELD, membership.getMailboxId().toString().toUpperCase(Locale.ENGLISH), Store.YES));
        doc.add(new LongField(UID_FIELD, membership.getUid(), Store.YES));

        // create an unqiue key for the document which can be used later on updates to find the document
        doc.add(new StringField(ID_FIELD, membership.getMailboxId().toString().toUpperCase(Locale.ENGLISH) + "-" + Long.toString(membership.getUid()), Store.YES));
        doc.add(new LongField(SIZE_FIELD, membership.getFullContentOctets(), Store.YES));

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
                    doc.add(new StringField(HEADERS_FIELD, fullValue, Store.NO));
                    doc.add(new StringField(PREFIX_HEADER_FIELD + headerName, headerValue, Store.NO));

                    if (f instanceof DateTimeField) {
                        // We need to make sure we convert it to GMT
                        final StringReader reader = new StringReader(f.getBody());
                        try {
                            DateTime dateTime = new DateTimeParser(reader).parseAll();

                            cal.set(dateTime.getYear(), dateTime.getMonth() - 1, dateTime.getDay(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
                            sentDate = cal.getTime();

                        } catch (org.apache.james.mime4j.field.datetime.parser.ParseException e) {
                            session.getLog().debug("Unable to parse Date header for proper indexing", e);
                            // This should never happen anyway fallback to the already parsed field
                            sentDate = ((DateTimeField) f).getDate();
                        }

                    }
                    String field = null;
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
                                doc.add(new Field(field, value, Store.NO, Index.ANALYZED));
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
                                    doc.add(new Field(field, value, Store.NO, Index.ANALYZED));

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


                        doc.add(new Field(field, headerValue, Store.NO, Index.ANALYZED));

                    } else if (headerName.equalsIgnoreCase("Subject")) {
                        doc.add(new Field(BASE_SUBJECT_FIELD, SearchUtil.getBaseSubject(headerValue), Store.YES, Index.NOT_ANALYZED));
                    }
                }
                if (sentDate == null) {
                    sentDate = membership.getInternalDate();
                } else {

                    doc.add(new Field(SENT_DATE_FIELD_YEAR_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.YEAR), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_MONTH_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MONTH), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_DAY_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.DAY), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_HOUR_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.HOUR), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_MINUTE_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MINUTE), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_SECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.SECOND), Store.NO, Index.NOT_ANALYZED));
                    doc.add(new Field(SENT_DATE_FIELD_MILLISECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MILLISECOND), Store.NO, Index.NOT_ANALYZED));

                }
                doc.add(new Field(SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MILLISECOND), Store.NO, Index.NOT_ANALYZED));

                doc.add(new Field(FIRST_FROM_MAILBOX_NAME_FIELD, firstFromMailbox, Store.YES, Index.NOT_ANALYZED));
                doc.add(new Field(FIRST_TO_MAILBOX_NAME_FIELD, firstToMailbox, Store.YES, Index.NOT_ANALYZED));
                doc.add(new Field(FIRST_CC_MAILBOX_NAME_FIELD, firstCcMailbox, Store.YES, Index.NOT_ANALYZED));
                doc.add(new Field(FIRST_FROM_MAILBOX_DISPLAY_FIELD, firstFromDisplay, Store.YES, Index.NOT_ANALYZED));
                doc.add(new Field(FIRST_TO_MAILBOX_DISPLAY_FIELD, firstToDisplay, Store.YES, Index.NOT_ANALYZED));

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
                    String line = null;
                    while ((line = bodyReader.readLine()) != null) {
                        doc.add(new Field(BODY_FIELD, line.toUpperCase(Locale.ENGLISH), Store.NO, Index.ANALYZED));
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
            parser.parse(membership.getFullContent());
        } catch (MimeException e) {
            // This should never happen as it was parsed before too without problems.
            throw new MailboxException("Unable to index content of message", e);
        } catch (IOException e) {
            // This should never happen as it was parsed before too without problems.
            // anyway let us just skip the body and headers in the index
            throw new MailboxException("Unable to index content of message", e);
        }


        return doc;
    }

    /**
     * @see org.apache.james.mailbox.store.search.ListeningMessageSearchIndex#add(org.apache.james.mailbox.MailboxSession, org.apache.james.mailbox.store.mail.model.Mailbox, org.apache.james.mailbox.store.mail.model.Message)
     */
    public void add(MailboxSession session, Mailbox<Id> mailbox, Message<Id> membership) throws MailboxException {
        Document doc = createMessageDocument(session, membership);
        Document flagsDoc = createFlagsDocument(membership);

        try {
            writer.storeMail(doc);
            writer.storeMail(flagsDoc);
        } catch (CorruptIndexException e) {
            throw new MailboxException("Unable to add message to index", e);
        } catch (IOException e) {
            throw new MailboxException("Unable to add message to index", e);
        }
    }

    @Override
    public void delete(MailboxSession session, Mailbox<Id> idMailbox, MessageRange range) throws MailboxException {
        // delete message from Index (at least mark it as deleted)
    }

    @Override
    public void update(MailboxSession session, Mailbox<Id> idMailbox, MessageRange range, Flags flags) throws MailboxException {
        // update message index (only message metadata changes: flags)
    }

    /**
     * Index the {@link Flags} and add it to the {@link Document}
     *
     * @param message
     */
    private Document createFlagsDocument(Message<?> message) {
        Document doc = new Document();
        doc.add(new Field(ID_FIELD, "flags-" + message.getMailboxId().toString() + "-" + Long.toString(message.getUid()), Store.YES, Index.NOT_ANALYZED));
        doc.add(new Field(MAILBOX_ID_FIELD, message.getMailboxId().toString(), Store.YES, Index.NOT_ANALYZED));
        doc.add(new LongField(UID_FIELD, message.getUid(), Store.YES));

        indexFlags(doc, message.createFlags());
        return doc;
    }

    /**
     * Add the given {@link Flags} to the {@link Document}
     *
     * @param doc
     * @param f
     */
    private void indexFlags(Document doc, Flags f) {
        List<String> fString = new ArrayList<String>();
        Flag[] flags = f.getSystemFlags();
        for (int a = 0; a < flags.length; a++) {
            fString.add(toString(flags[a]));
            doc.add(new Field(FLAGS_FIELD, toString(flags[a]), Store.NO, Index.NOT_ANALYZED));
        }

        String[] userFlags = f.getUserFlags();
        for (int a = 0; a < userFlags.length; a++) {
            doc.add(new Field(FLAGS_FIELD, userFlags[a], Store.NO, Index.NOT_ANALYZED));
        }

        // if no flags are there we just use a empty field
        if (flags.length == 0 && userFlags.length == 0) {
            doc.add(new Field(FLAGS_FIELD, "", Store.NO, Index.NOT_ANALYZED));
        }

    }
}
