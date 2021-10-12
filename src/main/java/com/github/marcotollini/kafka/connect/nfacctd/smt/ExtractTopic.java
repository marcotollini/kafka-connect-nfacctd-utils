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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ExtractTopic<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Insert a random UUID into a connect record";

  private interface ConfigName {
    String FIELD_NAMES = "fields";
    String FIELD_SEPARATOR = "separator";
    String SKIP_MISSING_OR_NULL_CONFIG  = "skip.missing.or.null";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.FIELD_NAMES, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
      "List of fields comma-separated")
    .define(ConfigName.SKIP_MISSING_OR_NULL_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
      "Skip NULL or missing")
    .define(ConfigName.FIELD_SEPARATOR, ConfigDef.Type.STRING, "_", ConfigDef.Importance.HIGH,
      "Separator that should match");

  private static final String PURPOSE = "adding UUID to record";

  private List<String> fieldNames;
  private String separator;
  private boolean skipMissingOrNull;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    String fieldNamesComma = config.getString(ConfigName.FIELD_NAMES);
    fieldNames = Arrays.asList(fieldNamesComma.split(","));
    separator = config.getString(ConfigName.FIELD_SEPARATOR);
    skipMissingOrNull = config.getBoolean(ConfigName.SKIP_MISSING_OR_NULL_CONFIG);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    ArrayList<String> topicValues = new ArrayList<String>();
    for (String fieldName: fieldNames){
      if (!value.containsKey(fieldName)){
        continue;
      }
      String fieldValue = (String) value.get(fieldName);
      if(fieldValue != null){
        topicValues.add(fieldValue);
      }
    }


    if (!skipMissingOrNull || topicValues.size() > 0){
      final String newTopic = String.join(separator, topicValues);
      return newRecord(record, newTopic);
    }else{
      return record;
    }


  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    ArrayList<String> topicValues = new ArrayList<String>();
    for (String fieldName: fieldNames){
      if (value.schema().field(fieldName) == null || (!value.schema().field(fieldName).schema().equals(Schema.OPTIONAL_STRING_SCHEMA) && !value.schema().field(fieldName).schema().equals(Schema.STRING_SCHEMA))){
        continue;
      }
      String fieldValue = (String) value.get(fieldName);
      if(fieldValue != null){
        topicValues.add(fieldValue);
      }
    }

    if (!skipMissingOrNull || topicValues.size() > 0){
      final String newTopic = String.join(separator, topicValues);
      return newRecord(record, newTopic);
    }else{
      return record;
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getRandomUuid() {
    return UUID.randomUUID().toString();
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected R newRecord(R record, String newTopic) {
    return record.newRecord(
        newTopic,
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        record.value(),
        record.timestamp(),
        record.headers()
      );
  }

  public static class Key<R extends ConnectRecord<R>> extends ExtractTopic<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ExtractTopic<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }
  }
}


