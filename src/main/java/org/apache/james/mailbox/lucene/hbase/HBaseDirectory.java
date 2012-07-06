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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.*;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;

public class HBaseDirectory extends Directory implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseDirectory.class);
    private boolean DIRECTORY_STATE_OPEN = false;
    private final Configuration config;

    public Configuration getConfig() {
        return config;
    }

    public HBaseDirectory() {
        this(HBaseConfiguration.create());
    }

    public HBaseDirectory(Configuration config) {
        this.config = checkNotNull(config);
        DIRECTORY_STATE_OPEN = true;
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(config);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }

        try {
            if (admin.tableExists(SEGMENTS_TABLE.name)){ //deleting previous data
                admin.disableTable(SEGMENTS_TABLE.name);
                admin.deleteTable(SEGMENTS_TABLE.name);
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(SEGMENTS_TABLE.name);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(TERM_DOCUMENT_CF.name);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            setLockFactory(NoLockFactory.getNoLockFactory());
        } catch (IOException e) {
            // Cannot happen
        }
    }

    @Override
    public String[] listAll() throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        List<String> files = Lists.newArrayList();
        HTable table = null;
        ResultScanner scanner = null;
        try {
            table = new HTable(config, SEGMENTS_TABLE.name);
            Scan scan = new Scan();
            // this way we're getting all of the columns
            scan.addFamily(TERM_DOCUMENT_CF.name);
            scanner = table.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                files.add(Bytes.toString(result.getRow()));
            }
        } catch (IOException e) {
            LOG.info("Exception reading from HBase", e);
            Throwables.propagate(e);
        } finally {
            Closeables.closeQuietly(scanner);
            Closeables.closeQuietly(table);
        }
        return files.toArray(new String[files.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        HTable table = null;
        try {
            table = new HTable(config, SEGMENTS_TABLE.name);
            Get get = new Get(Bytes.toBytes(name));
            Result result = table.get(get);
            if (result.isEmpty()) {
                return false;
            }
        } catch (IOException e) {
            LOG.info("Exception reading from HBase", e);
            Throwables.propagate(e);
        } finally {
            Closeables.closeQuietly(table);
        }
        return true;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        HTable table = null;
        try {
            table = new HTable(config, SEGMENTS_TABLE.name);
            Delete delete = new Delete(Bytes.toBytes(name));
            table.delete(delete);
            table.flushCommits();
        } catch (IOException e) {
            LOG.info("Exception reading from HBase", e);
            Throwables.propagate(e);
        } finally {
            Closeables.closeQuietly(table);
        }
    }

    @Override
    public final long fileLength(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        // if we store the file length in HBase we can return only that, without transfering the file
        IndexInput indexInput = new HIndexInput(name);
        long len = indexInput.length();
        indexInput.close();
        return len;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        return new HIndexOutput(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        //TODO: implement sync for commit action
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        return new HIndexInput(name);
    }

    @Override
    public void close() throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        DIRECTORY_STATE_OPEN = true;
    }

    /**
     * Stores the output in a byte array and flushes that to HBase. Inefficient.
     */
    protected class HIndexOutput extends IndexOutput {

        private final String name;
        private byte[] fileContents = new byte[0];
        private boolean STREAM_STATE_OPEN = false;
        private int bufferPosition = 0; // position in buffer

        public HIndexOutput(String name) {
            this.name = checkNotNull(name);
            STREAM_STATE_OPEN = true;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            checkState(STREAM_STATE_OPEN);
            if (bufferPosition == fileContents.length) { //add new byte
                fileContents = Bytes.add(fileContents, new byte[]{b});
                bufferPosition = fileContents.length;
            } else //overwriting
                fileContents[bufferPosition++] = b;
        }


        /**
         * we copy the bytes from the buffer and write it to the end of the file
         */
        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            checkState(STREAM_STATE_OPEN);
            checkPositionIndex(offset, b.length);

            fileContents = Bytes.add(fileContents, Arrays.copyOfRange(b, offset, length));
            bufferPosition = fileContents.length;
        }

        @Override
        public void close() throws IOException {
            checkState(STREAM_STATE_OPEN);
            flush();
            STREAM_STATE_OPEN = false;
        }

        @Override
        public long getFilePointer() {
            checkState(STREAM_STATE_OPEN);
            return bufferPosition;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkState(STREAM_STATE_OPEN);
            checkPositionIndex((int) pos, fileContents.length);
            bufferPosition = (int) pos;
        }

        @Override
        public long length() throws IOException {
            checkState(STREAM_STATE_OPEN);
            return fileContents.length;
        }

        /**
         * Forces any buffered output to be written.
         */
        @Override
        public void flush() throws IOException {
            checkState(STREAM_STATE_OPEN);
            HTable table = null;
            try {
                table = new HTable(config, SEGMENTS_TABLE.name);
                Put put = new Put(Bytes.toBytes(name));
//                LOG.info("Bytes in put {}", Arrays.toString(fileContents));
                put.add(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name, fileContents);
                table.put(put);
                table.flushCommits();
//                LOG.info("Writing to HBase {}", put.toJSON());
            } catch (IOException e) {
                LOG.info("Exception flushing file to HBase", e);
                Throwables.propagate(e);
            } finally {
                Closeables.closeQuietly(table);
                bufferPosition = 0;
            }
        }
    }

    protected class HIndexInput extends IndexInput {
        // we read the whole segment in memory in this array
        private byte[] fileContents;
        private boolean STREAM_STATE_OPEN = false;
        private int pointerInBuffer = 0;

        private HIndexInput(String name) {
            super(name);
            HTable table = null;
            try {
                table = new HTable(config, SEGMENTS_TABLE.name);
                Get get = new Get(Bytes.toBytes(name));
                Result result = table.get(get);
                // it is an error if the segment does not exist
                if (result.isEmpty()) {
                    if (STREAM_STATE_OPEN) {
                        throw new IllegalStateException();
                    }
                } else {
                    // get the bytes from the column we store them in
                    fileContents = result.getValue(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name);
//                    LOG.info("Read from HBase {} ", Arrays.toString(fileContents));
                    STREAM_STATE_OPEN = true;
                }
            } catch (IOException e) {
                LOG.info("Exception reading from HBase: ", e);
                Throwables.propagate(e);
            } finally {
                Closeables.closeQuietly(table);
            }
        }

        @Override
        public void close() throws IOException {
            // operations are possible only when the IndexInput is in state open
            if (STREAM_STATE_OPEN)
                STREAM_STATE_OPEN = false;
        }

        @Override
        public long getFilePointer() {
            checkState(STREAM_STATE_OPEN);
            return pointerInBuffer;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkState(STREAM_STATE_OPEN);
            pointerInBuffer = (int) pos;
        }

        @Override
        public long length() {
            checkState(STREAM_STATE_OPEN);
            return fileContents.length;
        }

        @Override
        public byte readByte() throws IOException {
            if (STREAM_STATE_OPEN){
                checkPositionIndex(pointerInBuffer, fileContents.length);
                return fileContents[pointerInBuffer++];
            }
            return 'b';
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            checkState(STREAM_STATE_OPEN);
            checkPositionIndex(pointerInBuffer + len, fileContents.length);
            Bytes.putBytes(b, offset, fileContents, pointerInBuffer, len);
            pointerInBuffer += len;
        }
    }
}
