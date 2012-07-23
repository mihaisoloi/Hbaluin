/******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more         *
 * contributor license agreements. See the NOTICE file distributed with       *
 * this work for additional information regarding copyright ownership.        *
 * The ASF licenses this file to You under the Apache License, Version 2.0    *
 * (the "License"); you may not use this file except in compliance with       *
 * the License. You may obtain a copy of the License at                       *
 *                                                                            *
 * http://www.apache.org/licenses/LICENSE-2.0                                 *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the License is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 ******************************************************************************/

package org.apache.james.mailbox.lucene.hbase.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * contains a list of TermDocuments for every document ID
 */
public class TermDocuments {

    private Map<String, List<TermDocument>> documentMap;

    public TermDocuments(Map<String, List<TermDocument>> documentMap) {
        this.documentMap = documentMap;
    }

    /**
     * counts the total frequency of the term in all of the documents
     *
     * @param term
     * @return totalFrequency
     */
    public int getTermFrequency(String term) {
        int i = 0;
        for (String key : documentMap.keySet()) {
            if (key.equals(term))
                for (TermDocument termDocument : documentMap.get(key)) {
                    i += termDocument.getDocFrequency();
                }
        }
        return i;
    }

    /**
     * adding documents to the existing terms or creating new terms in new documents
     *
     * @param term that can be found with the frequency and position in the docs
     * @param doc
     */
    public void addDocument(String term, TermDocument doc) {
        if (documentMap.get(term) != null)
            documentMap.get(term).add(doc);
        else {
            List<TermDocument> termDocuments = new ArrayList<TermDocument>();
            termDocuments.add(doc);
            documentMap.put(term, termDocuments);
        }
    }

    /**
     * remove documents from all of the terms
     *
     * @param documents
     */
    public void removeDocuments(List<TermDocument> documents) {
        for (TermDocument document : documents) {
            removeDocument(document);
        }
    }

    /**
     * remove document from all of the terms
     *
     * @param document
     */
    public void removeDocument(TermDocument document) {
        for (String key : documentMap.keySet()) {
            if (documentMap.get(key).contains(document)) {
                documentMap.remove(document);
            }
        }
    }

    public TermDocument getDocument(String term, TermDocument document) {
        return null;
    }

    public Iterator<TermDocument> getDocumentIterator(String term){
        return documentMap.get(term).iterator();
    }

    public List<TermDocument> getDocuments(String term){
        return documentMap.get(term);
    }

    @Override
    public String toString() {
        return "TermDocuments{" +
                "documentMap=" + documentMap +
                '}';
    }
}
