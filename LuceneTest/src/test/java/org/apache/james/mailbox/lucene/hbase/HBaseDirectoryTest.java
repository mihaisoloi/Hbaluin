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

package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.SEGMENTS_TABLE;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.TERM_DOCUMENT_CF;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.CONTENTS_QUALIFIER;

public class HBaseDirectoryTest extends HBaseSetup {


    @Test
    public void testListAll() throws Exception {

    }

    @Test
    public void testFileExists() throws Exception {

    }

    @Test
    public void testFileModified() throws Exception {

    }

    @Test
    public void testTouchFile() throws Exception {

    }

    @Test
    public void testDeleteFile() throws Exception {

    }

    @Test
    public void testFileLength() throws Exception {

    }

    String fileName = "testFileName";
    String fileContents = "mihai";
    byte[] bytesToWrite = Bytes.toBytes(fileContents);

    @Test
    public void testCreateOutput() throws Exception {

        HBaseDirectory directory = new HBaseDirectory(CLUSTER.getConf());
        LOG.info("Created directory");
        IndexOutput io = directory.createOutput(fileName);
        io.writeBytes(bytesToWrite, bytesToWrite.length);
        io.close();
        LOG.info("Wrote the file, checking if it exists");

        HTable hTable = new HTable(CLUSTER.getConf(), SEGMENTS_TABLE.name);
        Get get = new Get(toBytes(fileName));
        get.addColumn(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name);
        Result result = hTable.get(get);
        byte[] fileBytes = result.getValue(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name);
        hTable.close();

        assertEquals("Files are equal", fileContents, Bytes.toString(fileBytes));
    }

    @Test
    public void testOpenInput() throws Exception {
        HBaseDirectory directory = new HBaseDirectory(CLUSTER.getConf());
        LOG.info("Created directory");
        HTable hTable = new HTable(CLUSTER.getConf(), SEGMENTS_TABLE.name);
        Put put = new Put(toBytes(fileName));
        put.add(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name, bytesToWrite);
        hTable.put(put);
        hTable.flushCommits();
        LOG.info("Wrote the file, checking if it exists");

        IndexInput indexInput = directory.openInput(fileName);
        byte[] fileBytes = new byte[(int) indexInput.length()];
        indexInput.readBytes(fileBytes, 0, fileBytes.length);

        assertEquals("Files are equal", fileContents, Bytes.toString(fileBytes));
    }

    @Test
    public void testClose() throws Exception {

    }
}
