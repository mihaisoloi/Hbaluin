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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

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

    @Test
    public void testCreateOutput() throws Exception {
        String key="testFileName";
        String content="mihai";
        byte[] bytesToWrite = Bytes.toBytes(content);

        HBaseDirectory directory = new HBaseDirectory(CLUSTER.getConf());
        IndexOutput io = directory.createOutput(key);
        io.writeBytes(bytesToWrite,bytesToWrite.length);
        System.out.println(io.length()+"~~~~~~~~~~~~~~"+io.getFilePointer());

//        assertTrue(directory.fileExists(key));
    }

    @Test
    public void testOpenInput() throws Exception {

    }

    @Test
    public void testClose() throws Exception {

    }
}
