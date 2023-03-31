package de.rewe.kafkatogcs.schema.conversion;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import de.rewe.kafkatogcs.schema.conversion.commons.IteratorsUtils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main function of this class is to convert a JsonSchema to Avro schema. It can also
 * standardize schema names, and keep track of a mapping from the original names to the standardized
 * ones, which is needed for unit tests. <br/>
 */
public class JsonToAvroSchemaConverter {

  private static final String TYPE = "type";
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonToAvroSchemaConverter.class);

  static List<JsonSchemaType> getNonNullTypes(
      final String fieldName,
      final JsonNode fieldDefinition) {
    return getTypes(fieldName, fieldDefinition).stream()
        .filter(type -> type != JsonSchemaType.NULL).collect(toList());
  }

  /**
   * When no type is specified, it will default to string.
   */
  static List<JsonSchemaType> getTypes(final String fieldName, final JsonNode fieldDefinition) {
    final Optional<JsonNode> combinedRestriction = getCombinedRestriction(fieldDefinition);
    if (combinedRestriction.isPresent()) {
      return List.of(JsonSchemaType.COMBINED);
    }

    final JsonNode typeProperty = fieldDefinition.get(TYPE);
    if (typeProperty == null || typeProperty.isNull()) {
      LOGGER.warn("Field \"{}\" has no type specification. It will default to string", fieldName);
      return List.of(JsonSchemaType.STRING);
    }

    if (typeProperty.isArray()) {
      return IteratorsUtils.toList(typeProperty.elements()).stream()
                           .map(s -> JsonSchemaType.fromJsonSchemaTypeName(s.asText()))
                           .collect(toList());
    }

    if (typeProperty.isTextual()) {
      return List.of(JsonSchemaType.fromJsonSchemaTypeName(typeProperty.asText()));
    }

    LOGGER.warn("Field \"{}\" has unexpected type {}. It will default to string.",
        fieldName,
        typeProperty);
    return List.of(JsonSchemaType.STRING);
  }

  static Optional<JsonNode> getCombinedRestriction(final JsonNode fieldDefinition) {
    if (fieldDefinition.has("anyOf")) {
      return Optional.of(fieldDefinition.get("anyOf"));
    }
    if (fieldDefinition.has("allOf")) {
      return Optional.of(fieldDefinition.get("allOf"));
    }
    if (fieldDefinition.has("oneOf")) {
      return Optional.of(fieldDefinition.get("oneOf"));
    }
    return Optional.empty();
  }

  /**
   * @return Avro schema based on the input {@code jsonSchema}.
   */
  public Schema getAvroSchema(final JsonNode jsonSchema,
                              final String streamName,
                              @Nullable final String namespace) {
    return getAvroSchema(jsonSchema, streamName, namespace, true, true, true);
  }

  /**
   * @param appendExtraProps Add default additional property field to the output Avro schema.
   * @param addStringToLogicalTypes Default logical type field to string.
   * @param isRootNode Whether it is the root field in the input Json schema.
   * @return Avro schema based on the input {@code jsonSchema}.
   */
  public Schema getAvroSchema(final JsonNode jsonSchema,
                              final String fieldName,
                              @Nullable final String fieldNamespace,
                              final boolean appendExtraProps,
                              final boolean addStringToLogicalTypes,
                              final boolean isRootNode) {
    final String stdName = AvroConstants.NAME_TRANSFORMER.getIdentifier(fieldName);
    final String stdNamespace = AvroConstants.NAME_TRANSFORMER.getNamespace(fieldNamespace);
    final SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(stdName);
    if (!stdName.equals(fieldName)) {
      LOGGER.warn("Schema name \"{}\" contains illegal character(s) and is standardized to \"{}\"",
          fieldName,
          stdName);
      builder.doc(
          String.format("%s",
              fieldName));
    }
    if (stdNamespace != null) {
      builder.namespace(stdNamespace);
    }

    final Optional<JsonNode> properties = Optional.ofNullable(jsonSchema.get("properties"));
    // object field with no "properties" will be handled by the default additional properties
    // field during object conversion; so it is fine if there is no "properties"
    final List<String> subfieldNames = properties
        .map(p -> IteratorsUtils.toList(p.fieldNames()))
        .orElseGet(List::of);

    final SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();

    for (final String subfieldName : subfieldNames) {
      final String stdFieldName = AvroConstants.NAME_TRANSFORMER.getIdentifier(subfieldName);
      final JsonNode subfieldDefinition = properties.map(p -> p.get(subfieldName)).orElseGet(
          JsonNodeFactory.instance::missingNode);
      final SchemaBuilder.FieldBuilder<Schema> fieldBuilder = assembler.name(stdFieldName);
      if (!stdFieldName.equals(subfieldName)) {
        LOGGER.warn("Field name \"{}\" contains illegal character(s) and is standardized to \"{}\"",
            subfieldName, stdFieldName);
        fieldBuilder.doc(String.format("%s",
            subfieldName));
      }
      final String subfieldNamespace = isRootNode
          // Omit the namespace for root level fields,
          // because it is directly assigned in the builder above.
          // This may not be the correct choice.
          ? null
          : (stdNamespace == null ? stdName : (stdNamespace + "." + stdName));
      fieldBuilder.type(parseJsonField(
              subfieldName,
              subfieldNamespace,
              subfieldDefinition,
              appendExtraProps,
              addStringToLogicalTypes))
          .withDefault(null);
    }

    return assembler.endRecord();
  }

  /**
   * Generate Avro schema for a single Json field type. For example:
   *
   * <pre>
   * "number" -> ["double"]
   * </pre>
   */
  Schema parseSingleType(final String fieldName,
                         @Nullable final String fieldNamespace,
                         final JsonSchemaType fieldType,
                         final JsonNode fieldDefinition,
                         final boolean appendExtraProps,
                         final boolean addStringToLogicalTypes) {
    Preconditions.checkState(
            fieldType != JsonSchemaType.NULL,
            "Null types should have been filtered out");

    final Schema fieldSchema;
    switch (fieldType) {
      case INTEGER:
      case LONG:
      case NUMBER:
      case BOOLEAN:
        fieldSchema = Schema.create(fieldType.getAvroType());
        break;
      case STRING:
        fieldSchema = getStringSchema(fieldType, fieldDefinition);
        break;
      case COMBINED:
        fieldSchema = getCombinedSchema(
            fieldName,
            fieldNamespace,
            fieldDefinition,
            appendExtraProps,
            addStringToLogicalTypes);
        break;
      case ARRAY:
        fieldSchema = getArraySchema(
            fieldName,
            fieldNamespace,
            fieldDefinition,
            appendExtraProps,
            addStringToLogicalTypes);
        break;
      case OBJECT:
        fieldSchema = getAvroSchema(
            fieldDefinition,
            fieldName,
            fieldNamespace,
            appendExtraProps,
            addStringToLogicalTypes,
            false);
        break;
      default:
        LOGGER.warn(
            "Field \"{}\" has invalid type definition: {}. It will default to string.",
            fieldName,
            fieldDefinition);
        fieldSchema = Schema.createUnion(NULL_SCHEMA, STRING_SCHEMA);
        break;
    }
    return fieldSchema;
  }

  private Schema getArraySchema(
      String fieldName,
      String fieldNamespace,
      JsonNode fieldDefinition,
      boolean appendExtraProps,
      boolean addStringToLogicalTypes) {
    final Schema fieldSchema;
    final JsonNode items = fieldDefinition.get("items");
    if (items == null) {
      LOGGER.warn("Array field \"{}\" does not specify the items type. It will default to an array of strings",
          fieldName);
      fieldSchema = Schema.createArray(Schema.createUnion(NULL_SCHEMA, STRING_SCHEMA));
    } else if (items.isObject()) {
      if (!items.has("type") || items.get("type").isNull()) {
        LOGGER.warn("Array field \"{}\" does not specify the items type. it will default to an array of strings",
            fieldName);
        fieldSchema = Schema.createArray(Schema.createUnion(NULL_SCHEMA, STRING_SCHEMA));
      } else {
        // Objects inside Json array has no names. We name it with the ".items" suffix.
        final String elementFieldName = fieldName + ".items";
        fieldSchema = Schema.createArray(parseJsonField(elementFieldName,
            fieldNamespace, items,
            appendExtraProps,
            addStringToLogicalTypes));
      }
    } else if (items.isArray()) {
      final List<Schema> arrayElementTypes =
          parseJsonTypeUnion(
              fieldName,
              fieldNamespace, (ArrayNode) items,
              appendExtraProps,
              addStringToLogicalTypes);
      arrayElementTypes.add(0, NULL_SCHEMA);
      fieldSchema = Schema.createArray(Schema.createUnion(arrayElementTypes));
    } else {
      LOGGER.warn("Array field \"{}\" has invalid items specification: {}. It will default to an array of strings.",
          fieldName, items);
      fieldSchema = Schema.createArray(Schema.createUnion(NULL_SCHEMA, STRING_SCHEMA));
    }
    return fieldSchema;
  }

  private Schema getCombinedSchema(
      String fieldName,
      String fieldNamespace,
      JsonNode fieldDefinition,
      boolean appendExtraProps,
      boolean addStringToLogicalTypes) {
    final Schema fieldSchema;
    final Optional<JsonNode> combinedRestriction = getCombinedRestriction(fieldDefinition);
    final List<Schema> unionTypes =
        parseJsonTypeUnion(fieldName, fieldNamespace, (ArrayNode) combinedRestriction.orElseGet(
            JsonNodeFactory.instance::arrayNode), appendExtraProps, addStringToLogicalTypes);
    fieldSchema = Schema.createUnion(unionTypes);
    return fieldSchema;
  }

  private static Schema getStringSchema(JsonSchemaType fieldType, JsonNode fieldDefinition) {
    final Schema fieldSchema;
    if (fieldDefinition.has("format")) {
      final String format = fieldDefinition.get("format").asText();
      if ("date-time".equals(format)) {
        fieldSchema = LogicalTypes.timestampMicros()
                                  .addToSchema(Schema.create(Schema.Type.LONG));
      } else if ("date".equals(format)) {
        fieldSchema = LogicalTypes.date()
                                  .addToSchema(Schema.create(Schema.Type.INT));
      } else if ("time".equals(format)) {
        fieldSchema = LogicalTypes.timeMicros()
                                  .addToSchema(Schema.create(Schema.Type.LONG));
      } else {
        fieldSchema = Schema.create(fieldType.getAvroType());
      }
    } else {
      fieldSchema = Schema.create(fieldType.getAvroType());
    }
    return fieldSchema;
  }

  /**
   * Take in a union of Json field definitions, and generate Avro field schema unions. For example:
   *
   * <pre>
   * ["number", { ... }] -> ["double", { ... }]
   * </pre>
   */
  List<Schema> parseJsonTypeUnion(final String fieldName,
                                  @Nullable final String fieldNamespace,
                                  final ArrayNode types,
                                  final boolean appendExtraProps,
                                  final boolean addStringToLogicalTypes) {
    final List<Schema> schemas = IteratorsUtils.toList(types.elements())
        .stream()
        .flatMap(definition -> getNonNullTypes(fieldName, definition).stream().flatMap(type -> {
          final String namespace = fieldNamespace == null
              ? fieldName
              : fieldNamespace + "." + fieldName;
          final Schema singleFieldSchema = parseSingleType(
              fieldName,
              namespace,
              type,
              definition,
              appendExtraProps,
              addStringToLogicalTypes);

          if (singleFieldSchema.isUnion()) {
            return singleFieldSchema.getTypes().stream();
          } else {
            return Stream.of(singleFieldSchema);
          }
        }))
        .distinct()
        .collect(toList());

    return mergeRecordSchemas(fieldName, fieldNamespace, schemas);
  }

  /**
   * If there are multiple object fields, those fields are combined into one Avro record. This is
   * because Avro does not allow specifying a tuple of types (i.e. the first element is type x, the
   * second element is type y, and so on). For example, the following Json field types:
   *
   * <pre>
   * [
   *   {
   *     "type": "object",
   *     "properties": {
   *       "id": { "type": "integer" }
   *     }
   *   },
   *   {
   *     "type": "object",
   *     "properties": {
   *       "id": { "type": "string" }
   *       "message": { "type": "string" }
   *     }
   *   }
   * ]
   * </pre>
   *
   * is converted to this Avro schema:
   *
   * <pre>
   * {
   *   "type": "record",
   *   "fields": [
   *     { "name": "id", "type": ["int", "string"] },
   *     { "name": "message", "type": "string" }
   *   ]
   * }
   * </pre>
   */
  List<Schema> mergeRecordSchemas(final String fieldName,
                                  @Nullable final String fieldNamespace,
                                  final List<Schema> schemas) {
    final LinkedHashMap<String, List<Schema>> recordFieldSchemas = new LinkedHashMap<>();
    final Map<String, List<String>> recordFieldDocs = new HashMap<>();

    final List<Schema> mergedSchemas = schemas.stream()
        // gather record schemas to construct a single record schema later on
        .peek(schema -> {
          if (schema.getType() == Schema.Type.RECORD) {
            for (final Schema.Field field : schema.getFields()) {
              recordFieldSchemas.putIfAbsent(field.name(), new LinkedList<>());
              recordFieldSchemas.get(field.name()).add(field.schema());
              if (field.doc() != null) {
                recordFieldDocs.putIfAbsent(field.name(), new LinkedList<>());
                recordFieldDocs.get(field.name()).add(field.doc());
              }
            }
          }
        })
        // remove record schemas because they will be merged into one
        .filter(schema -> schema.getType() != Schema.Type.RECORD)
        .collect(toList());

    // create one record schema from all the record fields
    if (!recordFieldSchemas.isEmpty()) {
      final SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(fieldName);
      if (fieldNamespace != null) {
        builder.namespace(fieldNamespace);
      }

      final SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();

      for (final Map.Entry<String, List<Schema>> entry : recordFieldSchemas.entrySet()) {
        final String subfieldName = entry.getKey();

        final SchemaBuilder.FieldBuilder<Schema> subfieldBuilder = assembler.name(subfieldName);
        final List<String> subfieldDocs =
            recordFieldDocs.getOrDefault(subfieldName, List.of());
        if (!subfieldDocs.isEmpty()) {
          subfieldBuilder.doc(String.join("; ", subfieldDocs));
        }
        final List<Schema> subfieldSchemas = entry.getValue().stream()
            .flatMap(schema -> schema.getTypes().stream()
                // filter out null and add it later on as the first element
                .filter(s -> !s.equals(NULL_SCHEMA)))
            .distinct()
            .collect(toList());
        // recursively merge schemas of a subfield
        // because they may include multiple record schemas as well
        final List<Schema> mergedSubfieldSchemas =
            mergeRecordSchemas(subfieldName, fieldNamespace, subfieldSchemas);
        mergedSubfieldSchemas.add(0, NULL_SCHEMA);
        subfieldBuilder.type(Schema.createUnion(mergedSubfieldSchemas)).withDefault(null);
      }

      mergedSchemas.add(assembler.endRecord());
    }

    return mergedSchemas;
  }

  /**
   * Take in a Json field definition, and generate a nullable Avro field schema. For example:
   *
   * <pre>
   * {"type": ["number", { ... }]} -> ["null", "double", { ... }]
   * </pre>
   */
  Schema parseJsonField(final String fieldName,
                        @Nullable final String fieldNamespace,
                        final JsonNode fieldDefinition,
                        final boolean appendExtraProps,
                        final boolean addStringToLogicalTypes) {
    // Filter out null types, which will be added back in the end.
    final List<Schema> nonNullFieldTypes = getNonNullTypes(fieldName, fieldDefinition)
        .stream()
        .flatMap(fieldType -> {
          final Schema singleFieldSchema =
              parseSingleType(
                  fieldName,
                  fieldNamespace,
                  fieldType,
                  fieldDefinition,
                  appendExtraProps,
                  addStringToLogicalTypes);
          if (singleFieldSchema.isUnion()) {
            return singleFieldSchema.getTypes().stream();
          } else {
            return Stream.of(singleFieldSchema);
          }
        })
        .distinct()
        .collect(toList());

    if (nonNullFieldTypes.isEmpty()) {
      return Schema.create(Schema.Type.NULL);
    } else {
      // Mark every field as nullable to prevent missing value exceptions from Avro / Parquet.
      if (!nonNullFieldTypes.contains(NULL_SCHEMA)) {
        nonNullFieldTypes.add(0, NULL_SCHEMA);
      }
      // Logical types are converted to a union of
      // logical type itself and string. The purpose is to
      // default the logical type field to a string,
      // if the value of the logical type field is invalid and
      // cannot be properly processed.
      if ((nonNullFieldTypes
          .stream().anyMatch(schema -> schema.getLogicalType() != null)) &&
          (!nonNullFieldTypes.contains(STRING_SCHEMA)) && addStringToLogicalTypes) {
        nonNullFieldTypes.add(STRING_SCHEMA);
      }
      return Schema.createUnion(nonNullFieldTypes);
    }
  }

}
