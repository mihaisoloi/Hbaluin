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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;

public class HBaseDirectory extends Directory implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseDirectory.class);
    private boolean DIRECTORY_STATE_OPEN = false;
    private final Configuration config;

    /**
     * Use constructor chaining this way !!
     */
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
            if (!admin.tableExists(SEGMENTS_TABLE.name)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(SEGMENTS_TABLE.name);
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(TERM_DOCUMENT_CF.name);
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            setLockFactory(new SingleInstanceLockFactory());
        } catch (IOException e) {
            // Cannot happen
        }
    }

    /**
     * Files are rows in HBase: list == Scan.
     *
     * @return
     * @throws IOException
     */
    @Override
    public String[] listAll() throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        List<String> files = Lists.newArrayList();
        HTable table = null;
        ResultScanner scanner = null;
        try {
            table = new HTable(config, SEGMENTS_TABLE.name);
            Scan scan = new Scan();
            // nu ne intereseaza doar valoarea rândului == rokey, asa aducem toate coloanele
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

    /**
     * looking for a certain segment file
     *
     * @param name
     * @return
     * @throws IOException
     */
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

    /**
     * Returns the time the named file was last modified.
     *
     * @throws IOException if the file does not exist
     */
    @Override
    @Deprecated
    public long fileModified(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        return 0L;
    }

    /**
     * Set the modified time of an existing file to now.
     *
     * @throws IOException if the file does not exist
     * @deprecated Lucene never uses this API; it will be
     *             removed in 4.0.
     */
    @Override
    @Deprecated
    public void touchFile(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        // use checkAndPut() on a column if needed.
    }

    /**
     * Removes an existing file in the directory.
     *
     * @throws IOException if the file does not exist
     */
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

    /**
     * Returns the length in bytes of a file in the directory.
     *
     * @throws IOException if the file does not exist
     */
    @Override
    public final long fileLength(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        // if we store the file length in HBase we can return only that, without transfering the file
        IndexInput indexInput = new HIndexInput(name);
        long len = indexInput.length();
        indexInput.close();
        return len;
    }

    /**
     * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
     */
    @Override
    public IndexOutput createOutput(String name) throws IOException {
        checkState(DIRECTORY_STATE_OPEN);
        return new HIndexOutput(name);
    }

    /**
     * Returns a stream reading an existing file.
     */
    @Override
    public IndexInput openInput(String name) throws IOException {
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
        private int pointerInBuffer = -1;

        public HIndexOutput(String name) {
            this.name = checkNotNull(name);
            STREAM_STATE_OPEN = true;
        }

        @Override
        public void flush() throws IOException {
            checkState(STREAM_STATE_OPEN);
            HTable table = null;
            try {
                table = new HTable(config, SEGMENTS_TABLE.name);
                Put put = new Put(Bytes.toBytes(name));
                LOG.info("Bytes in put {}", Bytes.toString(fileContents));
                put.add(TERM_DOCUMENT_CF.name, CONTENTS_QUALIFIER.name, fileContents);
                table.put(put);
                LOG.info("Writing to HBase {}", put.toJSON());
            } catch (IOException e) {
                LOG.info("Exception flushing file to HBase", e);
                Throwables.propagate(e);
            } finally {
                Closeables.closeQuietly(table);
            }
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
            return pointerInBuffer < 0 ? 0 : pointerInBuffer;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkState(STREAM_STATE_OPEN);
            checkPositionIndex((int) pos, fileContents.length);
            pointerInBuffer = (int) pos;
        }

        @Override
        public long length() throws IOException {
            checkState(STREAM_STATE_OPEN);
            return fileContents.length;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            checkState(STREAM_STATE_OPEN);
            if (pointerInBuffer == fileContents.length) {
                fileContents = Bytes.add(fileContents, new byte[]{b});
            } else {
                pointerInBuffer = pointerInBuffer < 0 ? 0 : pointerInBuffer;
                fileContents[((int) pointerInBuffer++)] = b;
            }
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            checkState(STREAM_STATE_OPEN);
            checkPositionIndex(offset, fileContents.length);
            // overwriting bytes
            pointerInBuffer++;
            if (fileContents.length < offset + b.length) {
                fileContents = Bytes.add(Arrays.copyOfRange(fileContents, 0, offset), b);
            } else {
                // if the bytes left are not enough till the end, we copy what is left over
                while (length > 0) {
                    int remainInBuffer = fileContents.length - pointerInBuffer;
                    int bytesToCopy = length < remainInBuffer ? length : remainInBuffer;
                    System.arraycopy(b, offset, fileContents, pointerInBuffer, bytesToCopy);
                    offset += bytesToCopy;
                    length -= bytesToCopy;
                    pointerInBuffer += bytesToCopy;
                }
//                byte[] a = Arrays.copyOfRange(fileContents, 0, offset);
//                byte[] c = Arrays.copyOfRange(fileContents, offset + b.length, length);
//                fileContents = Bytes.add(a, b, c);
//                        Arrays.copyOfRange(fileContents, 0, offset),
//                        b,
//                        Arrays.copyOfRange(fileContents, offset + length-1, length)
//                );
            }
        }
    }

    protected class HIndexInput extends IndexInput {
        private String name;
        // we read the whole segment in memory in this array
        private byte[] fileContents;
        private boolean STREAM_STATE_OPEN = false;
        private long pointerInBuffer = 0;

        private HIndexInput(String name) {
            this.name = checkNotNull(name);
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
                    LOG.info("Read from HBase: ", Bytes.toString(fileContents));
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
            pointerInBuffer = pos;
        }

        @Override
        public long length() {
            checkState(STREAM_STATE_OPEN);
            return fileContents.length;
        }

        @Override
        public byte readByte() throws IOException {
            if (STREAM_STATE_OPEN)
                return fileContents[((int) pointerInBuffer++)];
            return 'b';
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            checkState(STREAM_STATE_OPEN);
            Bytes.putBytes(b, 0, fileContents, offset, len);
        }
    }
}
