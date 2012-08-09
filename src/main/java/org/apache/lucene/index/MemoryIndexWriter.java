package org.apache.lucene.index;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;

/**
 * scrie o instanta de Lucene Document in HBase
 */
public class MemoryIndexWriter {

    private final HBaseIndexStore storage;
    private final String primaryKey;
    private final Analyzer analyzer;

    public MemoryIndexWriter(HBaseIndexStore storage, final String primaryKey) throws IOException {

        this.storage = storage;
        this.primaryKey = primaryKey;
        this.analyzer = new SimpleAnalyzer(Version.LUCENE_40);
    }

    /**
     * writes the document as puts in HBase where the qualifier is composed of the documentId
     * which in our case is the mailID
     *
     * @param doc
     * @throws IOException
     */
    public void addDocument(Document doc) throws IOException {
        HTableInterface table = storage.getTable();
        int docId = doc.getField(this.primaryKey).numericValue().intValue();
        try {
//            String mailboxId = doc.get();
            for (IndexableField field : doc.getFields()) {
                FieldType fieldType= (FieldType) field.fieldType();
                Class<? extends Attribute> attribute = null;
                if (fieldType.numericType() == null) {
                    attribute = CharTermAttribute.class;
                }
                TokenStream tokens = null;
                try {
                    tokens = field.tokenStream(analyzer);
                    if (tokens == null)
                        tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));

                    tokens.addAttribute(attribute);
                    while (tokens.incrementToken()) {
                        CharTermAttribute charTermAttribute = (CharTermAttribute) tokens.getAttribute(attribute);
//                        storage.persistTerm(mailboxId, docId, field.name(),
//                                fieldType.numericType() == null ?
//                                        Bytes.toBytes(tokens.getAttribute(attribute).toString()) : Bytes.toBytes(field.numericValue().intValue()));
                    }
                    tokens.end();
                } finally {
                    tokens.close();
                }
            }
        } finally {
            table.flushCommits();
        }
    }

    public void updateDocument(Term term, Iterable<? extends IndexableField> doc) throws CorruptIndexException, IOException {

    }
}
