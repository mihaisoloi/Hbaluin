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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

/**
 * Avro does not support imports of subschema(s) into a schema document, this
 * class allows componentizing Avro Schemas, it's based on the fact that the
 * Schema class provides a toString() method, which returns a JSON string
 * representing the given schema definition
 * 
 * @author Boris Lublinsky
 */
public class AvroUtils {

    private static final Map<String, Schema> SCHEMAS = new HashMap<String, Schema>();

    public static void addSchema(String name, Schema schema) {
        SCHEMAS.put(name, schema);
    }

    public static Schema getSchema(String name) {
        return SCHEMAS.get(name);
    }

    /**
     * resolving is necessary to see if replacement of schema objects is
     * necessary in current schema
     * 
     * @param result
     * @return
     */
    public static String resolveSchema(String result) {

        for (Map.Entry<String, Schema> entry : SCHEMAS.entrySet())
            result = replace(result, entry.getKey(), entry.getValue()
                    .toString());
        return result;

    }

    /**
     * pattern is used in order to replace previous entered schema objects
     * 
     * @param str
     * @param pattern
     * @param replace
     * @return
     */
    static String replace(String str, String pattern, String replace) {

        int start = 0;
        int end = 0;
        StringBuilder result = new StringBuilder();
        while ((end = str.indexOf(pattern, start)) >= 0) {
            result.append(str.substring(start, end));
            result.append(replace);
            start = end + pattern.length();
        }
        result.append(str.substring(start));
        return result.toString();

    }

    /**
     * resolving and parsing schema
     * used for testing
     * 
     * @param schemaString
     * @return
     */
    public static Schema parseSchema(String schemaString) {

        String completeSchema = resolveSchema(schemaString);

        Schema schema = new Schema.Parser().parse(completeSchema);
        addSchema(schema.getFullName(), schema);
        return schema;

    }

    /**
     * 
     * resolving and parsing schema
     * receives InputStream
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public static Schema parseSchema(InputStream in) throws IOException {

        StringBuilder out = new StringBuilder();
        byte[] b = new byte[4096];
        for (int n; (n = in.read(b)) != -1;) {
            out.append(new String(b, 0, n));
        }
        return parseSchema(out.toString());

    }

    /**
     * reads one schema from File
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public static Schema parseSchema(File file) throws IOException {

        FileInputStream fis = new FileInputStream(file);
        return parseSchema(fis);
    }
}