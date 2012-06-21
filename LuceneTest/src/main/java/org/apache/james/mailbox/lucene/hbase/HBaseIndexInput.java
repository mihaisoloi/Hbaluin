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

import org.apache.commons.collections.MapIterator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class HBaseIndexInput extends IndexInput {

    private static Get get;
    private static HBaseFile file;
    private static String fileName;
    private static HTable hTable;

    public HBaseIndexInput(HBaseFile file,HTable hTable, Get get) {
        this.file=file;
        this.get=get;
        this.hTable = hTable;
    }

    public HBaseIndexInput(String name,HTable hTable, Get get) {
        this.fileName=name;
        this.get=get;
        this.hTable = hTable;
    }

    /**
     * Closes the stream to further operations.
     */
    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Returns the current position in this file, where the next read will
     * occur.
     *
     * @see #seek(long)
     */
    @Override
    public long getFilePointer() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Sets current position in this file, where the next read will occur.
     *
     * @see #getFilePointer()
     */
    @Override
    public void seek(long pos) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * The number of bytes in the file.
     */
    @Override
    public long length() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Reads and returns a single byte.
     *
     * @see org.apache.lucene.store.DataOutput#writeByte(byte)
     */
    @Override
    public byte readByte() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     * @see org.apache.lucene.store.DataOutput#writeBytes(byte[], int)
     */
    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        get.addColumn(HBaseNames.TERM_DOCUMENT_CF.name,HBaseNames.AVRO_QUALIFIER.name);
        Result result = hTable.get(get);
        NavigableMap<byte[], byte[]> myMap = result.getFamilyMap(HBaseNames.TERM_DOCUMENT_CF.name);

        for(byte[] keySet : myMap.navigableKeySet()){          //nu știu ce este exact in chei
            System.out.println("~~~~>>>"+Bytes.toString(keySet));
        }
        b=myMap.get(Bytes.toBytes("contents"));
    }
}
