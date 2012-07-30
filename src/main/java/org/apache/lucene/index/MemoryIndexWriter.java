package org.apache.lucene.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.james.mailbox.lucene.hbase.HBaseNames;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.CONTENTS_QUALIFIER;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.EMPTY_COLUMN_VALUE;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.INDEX_TABLE;

/**
 * scrie o instanta de Lucene Document in HBase
 */
public class MemoryIndexWriter {

    private FileSystem fs;
    private byte[] column = HBaseNames.COLUMN_FAMILY.name;
    private final String primaryKey;
    private final Analyzer analyzer;

    public MemoryIndexWriter(FileSystem fs, HBaseAdmin admin, final String primaryKey) throws IOException {
        this.fs = fs;

        HTableDescriptor htd = new HTableDescriptor(INDEX_TABLE.name);
        if (!htd.hasFamily(column)) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
            htd.addFamily(columnDescriptor);
        }
        if (!admin.tableExists(INDEX_TABLE.name)) {
            admin.createTable(htd);
        }

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
        HTable table = null;
        int docId = doc.getField(this.primaryKey).numericValue().intValue();
        try {
            table = new HTable(fs.getConf(), INDEX_TABLE.name);

            for (IndexableField field : doc.getFields()) {
                FieldType fieldType = (FieldType) field.fieldType();
                Class<? extends Attribute> attribute = null;
                if (fieldType.numericType() != null) {
                    attribute = NumericTokenStream.NumericTermAttribute.class;
                } else {
                    attribute = CharTermAttribute.class;
                }
                TokenStream tokens = field.tokenStream(analyzer);
                if (tokens == null)
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));

                tokens.addAttribute(attribute);
                while (tokens.incrementToken()) {
                    String term = field.name() + "/" + tokens.getAttribute(attribute).toString();

                    //row = document field/term
                    Put put = new Put(Bytes.toBytes(docId + "-" + field.name() + "/" + term));

                    // family=column_family, qualifier = mailbox uid,value = Term Documents
                    put.add(column, CONTENTS_QUALIFIER.name, EMPTY_COLUMN_VALUE.name);
                    table.put(put);
                }
                tokens.end();
                tokens.close();
            }
        } finally {
            table.flushCommits();
        }
    }

    /**
     * tokenizes the field and obtains the terms
     *
     * @param field
     * @return
     */
    private Set<Term> getTerms(IndexableField field) {
        StringTokenizer tokenizer = new StringTokenizer(field.stringValue());
        Set<Term> tokens = new HashSet<Term>();
        while (tokenizer.hasMoreTokens())
            tokens.add(new Term(tokenizer.nextToken()));
        return tokens;
    }

}
