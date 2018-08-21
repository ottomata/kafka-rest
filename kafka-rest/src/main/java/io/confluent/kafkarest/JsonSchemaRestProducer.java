/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafkarest;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.confluent.kafkarest.entities.JsonTopicProduceRecord;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.RestException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;

//import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.entities.ProduceRecord;
//import io.confluent.kafkarest.entities.SchemaHolder;
//import io.confluent.rest.exceptions.RestException;


import com.github.fge.jsonschema.core.load.SchemaLoader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaRestProducer implements RestProducer<JsonNode, JsonNode> {

  protected final KafkaProducer<JsonNode, JsonNode> producer;
  protected final KafkaJsonSerializer keySerializer;
  protected final KafkaJsonSerializer valueSerializer;
  // Maps JSONSchema URIs to their parsed JsonSchema to use for JsonNode data validation.
  protected final Map<String, JsonSchema> jsonSchemaCache;

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaRestProducer.class);


  public JsonSchemaRestProducer(
      KafkaProducer<JsonNode, JsonNode> producer,
      KafkaJsonSerializer keySerializer,
      KafkaJsonSerializer valueSerializer
  ) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.jsonSchemaCache = new ConcurrentHashMap<>(100);
  }

  public void produce(
      ProduceTask task,
      String topic,
      Integer partition,
      Collection<? extends ProduceRecord<JsonNode, JsonNode>> records
  ) {


    for (ProduceRecord<JsonNode, JsonNode> record : records) {
      // get jsonschema uri out of record schema_field
      // get jsonschema from cache or lookup from repo
      // validate record against jsonschema, throw/collect error if not valid

      if (!record.getKey().isNull()) {
        try {
          validateJson(topic, record.getKey());
        } catch (ProcessingException e) {
          // TODO new Errors code
          throw new RestException("Key JSONSchema validation", 422, 40801, e);
        } catch (Exception e) {
          throw new RestException("Got exception while validating key against JSONSchema", 422, 40801, e);
        }
      }

      // TODO should we enforce non-null value?  probably.
      if (!record.getValue().isNull()) {
        try {
          validateJson(topic, record.getValue());
        } catch (ProcessingException e) {
          // TODO new Errors code
          throw new RestException("Value JSONSchema validation", 422, 40801, e);
        } catch (Exception e) {
          throw new RestException("Got exception while validating value against JSONSchema", 422, 40801, e);
        }
      }

      Integer recordPartition = partition;
      if (recordPartition == null) {
        recordPartition = record.partition();
      }
      producer.send(
              new ProducerRecord<>(topic, recordPartition, record.getKey(), record.getValue()),
              task.createCallback()
      );
    }
  }

  public void close() {
    producer.close();
  }


  private String schemaUriPrefix = "http://localhost:8085/v1/schemas/";
  private String schemaUriSuffix = "";

  private JsonPointer schemaUriPointer = JsonPointer.compile("/meta/schema_uri");
  private Pattern schemaUriVersionPattern = Pattern.compile("([\\w\\-\\./:@]+)/(?<version>\\d+)");

  private final SchemaLoader schemaLoader = new SchemaLoader();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final YAMLFactory  yamlFactory  = new YAMLFactory();
  private final JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.byDefault();

  /**
   * Extracts the json value's JSONSchema URI from the schemaURIPointer json pointer.
   *
   * @param topic topic
   * @param value value
   * @return uri
   * @throws Exception e
   */
  public URI getSchemaUri(String topic, JsonNode value) throws Exception {
    try {
      return new URI(schemaUriPrefix + value.at(schemaUriPointer).textValue() + schemaUriSuffix);
    } catch (java.net.URISyntaxException e) {
      throw new Exception("Could not extract JSONSchema URI in field " + schemaUriPointer
        + " json value with prefix " + schemaUriPrefix, e);
    }
  }

  /**
   * Given a schemaURI, this will request the JSON or YAML content at that URI and
   * parse it into a JsonNode.  $refs will be resolved.
   *
   * @param schemaUri uri
   * @return schema
   * @throws Exception e
   */
  public JsonSchema getJsonSchema(String topic, URI schemaUri) throws Exception {
    JsonSchema schema = jsonSchemaCache.get(schemaUri.toString());
    if (schema != null)
      return schema;

    YAMLParser yamlParser = null;
    try {
      log.info("Looking up schema at " + schemaUri);
      yamlParser = yamlFactory.createParser(schemaUri.toURL());
    } catch (IOException e) {
      throw new Exception("Failed parsing json schema returned from " + schemaUri, e);
    }

    try {
      // TODO get fancy and use URITranslator to resolve relative $refs somehow?

      // Use SchemaLoader so we resolve any JsonRefs in the JSONSchema.
      JsonNode jsonSchemaNode = schemaLoader.load(objectMapper.readTree(yamlParser)).getBaseNode();
      log.debug("Retrieved JSONSchema JSON from " + schemaUri + ":\n" + jsonSchemaNode);
      schema = jsonSchemaFactory.getJsonSchema(jsonSchemaNode);

      // cache this schema for later if it is a versioned schema
      // TODO we should probalby always cache, not sure what to do with unversioned schema uri.
      // perhaps this should not happen.
      if (getSchemaVersion(topic, schemaUri.toString()) != null)
        jsonSchemaCache.put(schemaUri.toString(), schema);

      return schema;

      // TODO fix Exception types
    } catch (IOException e) {
      throw new Exception("Failed reading json schema returned from " + schemaUri, e);
    }
  }

  /**
   * Extracts the schema version from the schemaURIString using the schemaURIVersionRegex.
   *
   * @param topic Unused here, but a subclass could override this to use the topic when
   *              infering the schema version.
   *
   * @param schemaURIString
   * @return
   */
  public Integer getSchemaVersion(String topic, String schemaURIString) throws Exception {
    Matcher versionMatcher = schemaUriVersionPattern.matcher(schemaURIString);

    Integer version = null;

    // If we matched a schema version,
    // then extract it from the match and parse it as an Integer.
    if (versionMatcher.find()) {
      String versionString = versionMatcher.group("version");
      try {
        version = Integer.parseInt(versionString);
        log.trace("Extracted schema version " + version + " from schema URI " + schemaURIString);
      }
      catch (NumberFormatException e) {
        throw new Exception("Failed parsing schema version " + versionString + " as an Integer.", e);
      }
    }

    return version;
  }

  /**
   * Looks up the JSONSchema for the JSON data or topic and validates it.
   * @param data JsonNode data
   * @throws com.github.fge.jsonschema.core.exceptions.ProcessingException if validation fails
   */
  public void validateJson(String topic, JsonNode data) throws Exception {
    URI schemaUri = getSchemaUri(topic, data);
    JsonSchema schema = getJsonSchema(topic, schemaUri);
    ProcessingReport report = schema.validate(data);
    log.debug("JSON data validated against JSONSchema at " + schemaUri, report);
  }

}
