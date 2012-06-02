/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one *
 * or more contributor license agreements. See the NOTICE file *
 * distributed with this work for additional information *
 * regarding copyright ownership. The ASF licenses this file *
 * to you under the Apache License, Version 2.0 (the *
 * "License"); you may not use this file except in compliance *
 * with the License. You may obtain a copy of the License at *
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 *
 * 
 * Unless required by applicable law or agreed to in writing, *
 * software distributed under the License is distributed on an *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY *
 * KIND, either express or implied. See the License for the *
 * specific language governing permissions and limitations *
 * under the License. *
 ****************************************************************/
package org.apache.james.mailbox.lucene.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.ftpserver.util.IoUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author msoloi
 */
public class AvroInheritanceTest {

    private Schema fieldsDataSchema, termDocumentSchema,
            termDocumentFrequencySchema, schema, subSchema;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        subSchema = AvroUtils.parseSchema(new File(
                "resources/FacebookUser.avro"));
        schema = AvroUtils.parseSchema(new File(
                "resources/FacebookSpecialUser.avro"));
        fieldsDataSchema = AvroUtils.parseSchema(new File(
                "resources/FieldsData.avro"));
        termDocumentSchema = AvroUtils.parseSchema(new File(
                "resources/TermDocument.avro"));
        termDocumentFrequencySchema = AvroUtils.parseSchema(new File(
                "resources/TermDocumentFrequency.avro"));
    }

    /**
     * creates Schemas that have children
     * 
     * @throws Exception
     */
    @Test
    public void inheritanceTest() throws Exception {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                schema);
        Encoder encoder = EncoderFactory.get()
                .binaryEncoder(outputStream, null);
        GenericRecord child1 = new GenericData.Record(subSchema);
        child1.put("name", new Utf8("Doctor Who"));
        child1.put("num_likes", 1);
        child1.put("num_photos", 0);
        child1.put("num_groups", 423);
        GenericRecord parent1 = new GenericData.Record(schema);
        parent1.put("user", child1);
        parent1.put("specialData", 1);

        writer.write(parent1, encoder);

        GenericRecord child2 = new GenericData.Record(subSchema);
        child2.put("name", new org.apache.avro.util.Utf8("Doctor WhoWho"));
        child2.put("num_likes", 2);
        child2.put("num_photos", 0);
        child2.put("num_groups", 424);
        GenericRecord parent2 = new GenericData.Record(schema);
        parent2.put("user", child2);
        parent2.put("specialData", 2);

        writer.write(parent2, encoder);

        encoder.flush();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
                outputStream.toByteArray());
        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                schema);
        while (true) {
            try {
                GenericRecord result = reader.read(null, decoder);
                System.out.println(result);
            } catch (EOFException eof) {
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        IoUtils.close(inputStream);
        IoUtils.close(outputStream);
    }
}
