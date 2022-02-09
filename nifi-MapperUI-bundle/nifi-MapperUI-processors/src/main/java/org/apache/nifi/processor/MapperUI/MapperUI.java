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
package org.apache.nifi.processor.MapperUI;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@SupportsBatching
@Tags({"example"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Map JSON object to SQL create table statement.")
@WritesAttributes({
	@WritesAttribute(attribute="mime.type", description="Sets mime.type of FlowFile to sql")
})
public class MapperUI extends AbstractProcessor {
	
	public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the table to be created.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("A FlowFile is routed to this relationship when its contents have successfully been converted into a SQL statement")
            .build();
    static final Relationship REL_SQL = new Relationship.Builder()
            .name("sql")
            .description("A FlowFile is routed to this relationship when its contents have successfully been converted into a SQL statement")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be converted into a SQL statement. Common causes include invalid JSON "
                    + "content or the JSON content missing a required field.")
            .build();
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TABLE_NAME);
        return properties;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
    	final Set<Relationship> rels = new HashSet<>();
    	rels.add(REL_ORIGINAL);
    	rels.add(REL_SQL);
    	rels.add(REL_FAILURE);
    	return rels;
    }

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        
        String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        
        FlowFile sqlFlowFile = session.create(flowFile);
        
        sqlFlowFile = session.write(sqlFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(tableName.getBytes(StandardCharsets.UTF_8));
                out.close();
            }
        });
        
        session.transfer(sqlFlowFile, REL_SQL);
        session.transfer(flowFile, REL_ORIGINAL);
	}
}