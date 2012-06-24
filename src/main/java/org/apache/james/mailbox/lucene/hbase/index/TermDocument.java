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

import java.util.Arrays;

/**
 * contains the information to be stored in the index for a
 * given document (i.e. document frequency and array of positions)
 */
public class TermDocument {

    private int docFrequency;
    private int[] docPositions;

    public TermDocument(int docFrequency, int[] docPositions) {
        this.docFrequency = docFrequency;
        this.docPositions = docPositions;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    public int[] getDocPositions() {
        return docPositions;
    }

    @Override
    public String toString() {
        return "TermDocument{" +
                "docFrequency=" + docFrequency +
                ", docPositions=" + Arrays.toString(docPositions) +
                '}';
    }
}