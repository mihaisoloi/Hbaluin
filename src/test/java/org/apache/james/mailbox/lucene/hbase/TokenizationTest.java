package org.apache.james.mailbox.lucene.hbase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.Version;
import org.junit.Test;

import java.io.IOException;

public class TokenizationTest {

    private static final int UID = 1;
    private static final String text = "Tokenization is a pain in the behind. That is why I must write this test in order to get how it works.";

    @Test
    public void testTokenization() throws IOException {
        Document doc = new Document();
        doc.add(new IntField("uid", UID, Field.Store.NO));
        doc.add(new TextField("testText", text, Field.Store.NO));
        Analyzer analyzer = new SimpleAnalyzer(Version.LUCENE_40);
        for (IndexableField field : doc.getFields()) {
            TokenStream tokens = field.tokenStream(analyzer);
            FieldType fieldType = (FieldType) field.fieldType();
            Class<? extends Attribute> attribute = null;
            if (fieldType.numericType() != null) {
                attribute = NumericTokenStream.NumericTermAttribute.class;
            } else {
                attribute = CharTermAttribute.class;
            }
            while (tokens.incrementToken()) {
                String term = field.name() + "/" + tokens.getAttribute(attribute).toString();
                System.out.println(term);
            }
        }
    }

}
