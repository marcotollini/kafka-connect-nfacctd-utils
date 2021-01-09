/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.marcotollini.kafka.connect.nfacctd.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RegexNullifyTest {

  private RegexNullify<SourceRecord> xform = new RegexNullify.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test(expected = DataException.class)
  public void topLevelStructRequired() {
    xform.configure(Collections.singletonMap("fields", "myUuid"));
    xform.configure(Collections.singletonMap("regex", "tests"));
    xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
  }

  @Test
  public void copySchemaAndRegexNullifyFieldSingleEqual() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value");
    props.put("regex", "test");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("value", Schema.OPTIONAL_STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("value", "test");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void copySchemaAndRegexNullifyFieldSingleWrongType() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "magic");
    props.put("regex", "test");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
  }

  @Test
  public void copySchemaAndRegexNullifyFieldChangeSchema() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value");
    props.put("regex", "test");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("value", Schema.STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("value", "test");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void copySchemaAndRegexNullifyFieldSingleDiss() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value");
    props.put("regex", "nontest");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("value", Schema.OPTIONAL_STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("value", "test");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value").schema());
    assertEquals("test", ((Struct) transformedRecord.value()).getString("value"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void copySchemaAndRegexNullifyFieldMiltipleEqualAndDiff() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value1,value2,value3");
    props.put("regex", "test");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("value1", Schema.OPTIONAL_STRING_SCHEMA).field("value2", Schema.OPTIONAL_STRING_SCHEMA).field("value3", Schema.OPTIONAL_STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("value1", "test").put("value2", "test").put("value3", "nontest");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value1").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value1"));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value2").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value2"));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value3").schema());
    assertEquals("nontest", ((Struct) transformedRecord.value()).getString("value3"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void copySchemaAndRegexNullifyFieldMiltipleRegex() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value1,value2,value3");
    props.put("regex", "test.*");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("value1", Schema.OPTIONAL_STRING_SCHEMA).field("value2", Schema.OPTIONAL_STRING_SCHEMA).field("value3", Schema.OPTIONAL_STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L).put("value1", "test").put("value2", "test_something").put("value3", "nontest");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value1").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value1"));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value2").schema());
    assertEquals(null, ((Struct) transformedRecord.value()).getString("value2"));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("value3").schema());
    assertEquals("nontest", ((Struct) transformedRecord.value()).getString("value3"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void schemalessRegexNullifyField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("fields", "value");
    props.put("regex", "test");

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, Collections.singletonMap("value", "test"));

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals(null, ((Map) transformedRecord.value()).get("value"));
  }
}