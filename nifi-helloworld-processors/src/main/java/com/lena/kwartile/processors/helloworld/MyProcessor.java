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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.io.*;
import java.util.*;

import static org.apache.nifi.processor.util.put.AbstractPutEventProcessor.REL_SUCCESS;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MyProcessor extends AbstractProcessor {
    String json = "";
    Logger logger = LoggerFactory.getLogger(MyProcessor.class);

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("My Property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("my_relationship")
            .description("Example relationship")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted incoming CSV file to JSON")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to convert incoming CSV file to JSON")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private JAXBContext jaxbContext;
    private Unmarshaller unmarshaller;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        System.out.println("Inside MyProcessor.init " + context);
        logger.warn("Inside MyProcessor.init info statement ");
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        try {
            jaxbContext = JAXBContext.newInstance(ContactType.class);
            unmarshaller = jaxbContext.createUnmarshaller();
        } catch (Exception ex) {
            logger.error("Error in MyProcessor.init " + ex.getMessage(), ex);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        logger.warn("Inside MyProcessor.onScheduled context " + context);

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        logger.warn("Inside MyProcessor.onTrigger context " + context + " session " + session);

        if (flowFile == null) {
            return;
        }
        try {
            InputStream stream = session.read(flowFile);
            String xml = IOUtils.toString(stream, "UTF-8");
            stream.close();

            //TODO convert xml to java


            //TODO convert java to avro byte array

            logger.warn("Read a message in onTrigger " + xml);

            //  String json = "This would be replaced by avro";
            // String json = xml.toString();

            logger.info("Before unmarshaling xml");
            ContactType contactType = (ContactType) unmarshaller.unmarshal(new StringReader(xml));
            logger.info("After unmarshaling xml {}", contactType);
            logger.info("Before avro schema ****************************");
/*

            Schema.Parser parser = new Schema.Parser();
//       Schema schema = parser.parse(String.valueOf(getClass().getResource("/x.avsc")));
            Schema schema = parser.parse(getClass().getResourceAsStream("/contact.avsc"));
            GenericRecord datum = new GenericData.Record(schema);
            datum.put("contactId", contactType.getId());
            datum.put("firstname", contactType.getFirstname());
            datum.put("lastname",contactType.getLastname());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer =
                    new GenericDatumWriter<>(schema);
            Encoder encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null);
            writer.write(datum, encoder);
            encoder.flush();
            out.close();
            byte[] value1 =  out.toByteArray();*/

            logger.info("After avro schema ****************************");

            String firstname = contactType.getFirstname();
            logger.info("Value of first name ", firstname.toString());


            FlowFile output = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {
                    IOUtils.write(xml, outputStream, "UTF-8");
                }
            });
            // Change mime type to avro/binary
            output = session.putAttribute(output, CoreAttributes.MIME_TYPE.key(), "plain/text");
         //   output = session.putAttribute(output, CoreAttributes.MIME_TYPE.key(), "avro/binary");


            //TODO: May want to have a better default name....
            output = session.putAttribute(output, CoreAttributes.FILENAME.key(), UUID.randomUUID().toString() + ".json");
            session.transfer(output, REL_SUCCESS);

            // TODO implement

        } catch (Exception e) {
            logger.error("Error in MyProcessor.onTrigger " + e.getMessage(), e);
            logger.info("Returning failure");
            session.transfer(flowFile,REL_FAILURE);
        }


    }
}
