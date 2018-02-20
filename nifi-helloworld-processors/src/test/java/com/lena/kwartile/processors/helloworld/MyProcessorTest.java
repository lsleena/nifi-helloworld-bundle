/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lena.kwartile.processors.helloworld;

import com.sun.codemodel.internal.util.EncoderFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;


public class MyProcessorTest {

    private Logger logger = LoggerFactory.getLogger(MyProcessorTest.class);
    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public ContactType testProcessor() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(ContactType.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            ContactType contactType = (ContactType)
                    unmarshaller.unmarshal(new StringReader("<contact><contactid>1</contactid><firstname>Navya</firstname><lastname>Patil</lastname></contact>"));
            logger.info("ContactType {} ", contactType);
            return contactType;

        } catch (Exception ex) {
            logger.error("Error in MyProcessor.init " + ex.getMessage(), ex);
        }
        return null;
    }

   /* public void testProcessor1(){
         MyProcessorTest xsdHelper = new MyProcessorTest();
         ContactType contactType = xsdHelper.testProcessor();
         System.out.println("Parsed XML " + contactType.getFirstname() +" " + contactType.getLastname() );

         org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
     org.apache.avro.Schema schema = parser.parse(String.valueOf(getClass().getResource("/x.avsc")));
         javax.xml.validation.Schema schema = null;
         try {
             schema = parser.parse(getClass().getResourceAsStream("/contact.avsc"));
         } catch (IOException e) {
             e.printStackTrace();
         }
         GenericRecord datum = new GenericData.Record(schema);
         datum.put("contactId", contactType.getContactid());
         datum.put("firstname", contactType.getFirstname());
         datum.put("lastname",contactType.getLastname());
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         DatumWriter<GenericRecord> writer =
                 new GenericDatumWriter<>(schema);
         org.apache.avro.io.Encoder encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null);
         writer.write(datum, encoder);
         encoder.flush();
         out.close();
         byte[] value1 =  out.toByteArray();

    }*/

}
