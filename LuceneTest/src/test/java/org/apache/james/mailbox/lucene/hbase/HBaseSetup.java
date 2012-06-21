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

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.james.mailbox.lucene.avro.AvroUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.INDEX_TABLE;

public abstract class HBaseSetup {

    protected static final Logger LOG = LoggerFactory.getLogger(HBaseMiniClusterTesting.class);
    protected static final HBaseClusterSingleton CLUSTER = HBaseClusterSingleton
            .build();
    protected static HBaseAdmin admin = null;

    @Before
    public void setUp() throws IOException {
        CLUSTER.ensureTable(INDEX_TABLE.name, new byte[][]{COLUMN_FAMILY.name});
        admin = new HBaseAdmin(CLUSTER.getConf());
    }

    @After
    public void tearDown() {
        IOUtils.closeStream(admin);
    }

}
