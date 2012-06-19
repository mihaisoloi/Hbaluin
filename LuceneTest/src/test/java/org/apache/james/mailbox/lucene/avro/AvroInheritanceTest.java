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
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;
import org.apache.ftpserver.util.IoUtils;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @author msoloi
 */
public class AvroInheritanceTest {

    private Schema ext1, ext2, ext3, specialUser, baseUser;

    /**
     * 
     * instantiating the schemas with the definitions, they must be done in
     * order or the polymorphism won't work because they don't know about the
     * classes above
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        baseUser = AvroUtils
                .parseSchema(new File("LuceneTest/resources/test/avro/FacebookUser.avro"));
        ext1 = AvroUtils.parseSchema(new File(
                "LuceneTest/resources/test/avro/FacebookSpecialUserExtension1.avro"));
        ext2 = AvroUtils.parseSchema(new File(
                "LuceneTest/resources/test/avro/FacebookSpecialUserExtension2.avro"));
        ext3 = AvroUtils.parseSchema(new File(
                "LuceneTest/resources/test/avro/FacebookSpecialUserExtension3.avro"));
        specialUser = AvroUtils.parseSchema(new File(
                "LuceneTest/resources/test/avro/FacebookSpecialUser.avro"));
    }

    /**
     * creates Schemas that have children(i.e. polymorphism)
     * 
     * @throws Exception
     */
    @Test
    public void inheritanceTest() throws Exception {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get()
                .binaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                specialUser);

        for (int i = 0; i < 4; i++)
            populateUsers(writer, encoder, i);

        System.out.println(AvroUtils.getSchema(specialUser.getFullName())
                .toString(true));

        encoder.flush();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(
                outputStream.toByteArray());
        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                specialUser);

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

    public void populateUsers(GenericDatumWriter<GenericRecord> writer,
            Encoder encoder, int x) throws IOException {

        GenericRecord baseUserRecord = new GenericData.Record(baseUser);
        baseUserRecord.put("name", new Utf8("Doctor Who^" + x));
        baseUserRecord.put("num_likes", x);
        baseUserRecord.put("num_photos", x);
        baseUserRecord.put("num_groups", x);

        GenericRecord specialUserRecord = new GenericData.Record(specialUser);
        specialUserRecord.put("user", baseUserRecord);

        Schema extendedSchema = x == 1 ? ext1 : x == 2 ? ext2 : ext3;
        boolean write = x == 0 || specialUser.getField("extension")
                .schema().toString().contains(extendedSchema.getName());

        if (x == 0)
            specialUserRecord.put("type", "base");
        else if (write) {// for extensions, removes duplication
            GenericRecord extRecord = new GenericData.Record(extendedSchema);
            extRecord.put("specialData" + x, x);

            specialUserRecord.put("type", "extension" + x);
            specialUserRecord.put("extension", extRecord);
        }

        if (write)
            writer.write(specialUserRecord, encoder);
    }


    /*@Test
    public void insertAvroPut() throws IOException{

        //creating an AvroPut
        AColumnValue columnValue = new AColumnValue();
        columnValue.family = ByteBuffer.wrap(COLUMN_FAMILY.name);
        columnValue.qualifier = ByteBuffer.wrap(toBytes("varsta"));
        columnValue.value = ByteBuffer.wrap(toBytes(27));

        Schema columnValueSchema = Schema.createArray(AColumnValue.SCHEMA$);
        GenericArray<AColumnValue> columnValues = new GenericData.Array<AColumnValue>(5, columnValueSchema);
        columnValues.add(columnValue);

        APut put = new APut();
        put.row = ByteBuffer.wrap(toBytes("mihai"));
        put.columnValues = columnValues;

        //writing the put to the output stream
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream,null);
        SpecificDatumWriter<APut> writer = new SpecificDatumWriter<APut>(APut.SCHEMA$);
        writer.write(put,binaryEncoder);
        binaryEncoder.flush();

        byte[] bytes=byteArrayOutputStream.toByteArray();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream,null);
        SpecificDatumReader<AGet> reader = new SpecificDatumReader<AGet>(AGet.SCHEMA$);

        while (true) {
            try {
                AGet get = reader.read(null, binaryDecoder);
                System.out.println(get.row.toString());
            } catch (EOFException eof) {
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

//        assertEquals(put.row,get.row);
    }*/
}
