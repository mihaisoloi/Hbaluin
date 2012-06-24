/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one *
 * or more contributor license agreements. See the NOTICE file *
 * distributed with this work for additional information *
 * regarding copyright ownership. The ASF licenses this file *
 * to you under the Apache License, Version 2.0 (the *
 * "License"); you may not use this file except in compliance *
 * with the License. You may obtain a copy of the License at *
 * *
 * http://www.apache.org/licenses/LICENSE-2.0 *
 * *
 * Unless required by applicable law or agreed to in writing, *
 * software distributed under the License is distributed on an *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY *
 * KIND, either express or implied. See the License for the *
 * specific language governing permissions and limitations *
 * under the License. *
 ****************************************************************/
package org.apache.james.mailbox.lucene.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * testing the serialization of Avro binary protocol
 * 
 * @author ms
 * 
 */
public class AvroSerializationTest {

    private static final String schemaDescription = "{ \n"
            + " \"namespace\": \"com.navteq.avro\", \n"
            + " \"name\": \"FacebookUser\", \n" + " \"type\": \"record\",\n"
            + " \"fields\": [\n"
            + " {\"name\": \"name\", \"type\": [\"string\", \"null\"] },\n"
            + " {\"name\": \"num_likes\", \"type\": \"int\"},\n"
            + " {\"name\": \"num_photos\", \"type\": \"int\"},\n"
            + " {\"name\": \"num_groups\", \"type\": \"int\"} ]\n" + "}";

    private static final String schemaDescriptionExt = " { \n"
            + " \"namespace\": \"com.navteq.avro\", \n"
            + " \"name\": \"FacebookSpecialUser\", \n"
            + " \"type\": \"record\",\n"
            + " \"fields\": [\n"
            + " {\"name\": \"user\", \"type\": com.navteq.avro.FacebookUser },\n"
            + " {\"name\": \"specialData\", \"type\": \"int\"} ]\n" + "}";
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        Parser parser = new Schema.Parser();
        schema = parser.parse(schemaDescription);
    }

    @Test
    public void parseSchema() {

        Assert.assertEquals("com.navteq.avro.FacebookUser",
                schema.getFullName());
        System.out.println(schema.toString(true));
    }

    @Test
    public void avroUtilsResolveParsing() {
        AvroUtils.addSchema(schema.getFullName(), schema);
        Schema extended = AvroUtils.parseSchema(schemaDescriptionExt);
        System.out.println(extended.toString(true));
    }
}
