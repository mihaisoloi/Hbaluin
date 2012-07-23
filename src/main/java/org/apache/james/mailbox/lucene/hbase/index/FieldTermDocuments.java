package org.apache.james.mailbox.lucene.hbase.index;

import java.util.Collection;
import java.util.NavigableMap;

public class FieldTermDocuments {

    private NavigableMap<Integer, TermDocuments> termDocs;

    public FieldTermDocuments(NavigableMap<Integer, TermDocuments> termDocs) {
        this.termDocs = termDocs;
    }

    public Collection<TermDocuments> getDocuments(){
        return termDocs.values();
    }

}
