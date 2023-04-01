package de.rewe.kafkatogcs.schema.conversion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions
import org.slf4j.LoggerFactory
import java.util.Optional
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

/**
 * The main function of this class is to convert a JsonSchema to Avro schema. It can also
 * standardize schema names, and keep track of a mapping from the original names to the standardized
 * ones, which is needed for unit tests. <br></br>
 */
class JsonToAvroSchemaConverter {
    /**
     * @return Avro schema based on the input `jsonSchema`.
     */
    fun getAvroSchema(
        jsonSchema: JsonNode,
        streamName: String,
        namespace: String?
    ): Schema {
        return getAvroSchema(
            jsonSchema = jsonSchema,
            fieldName = streamName,
            fieldNamespace = namespace,
            appendExtraProps = true,
            addStringToLogicalTypes = true,
            isRootNode = true
        )
    }

    /**
     * @param appendExtraProps Add default additional property field to the output Avro schema.
     * @param addStringToLogicalTypes Default logical type field to string.
     * @param isRootNode Whether it is the root field in the input Json schema.
     * @return Avro schema based on the input `jsonSchema`.
     */
    fun getAvroSchema(
        jsonSchema: JsonNode,
        fieldName: String,
        fieldNamespace: String?,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean,
        isRootNode: Boolean
    ): Schema {
        val stdName = AvroConstants.NAME_TRANSFORMER.getIdentifier(fieldName)
        val stdNamespace = fieldNamespace?.let { AvroConstants.NAME_TRANSFORMER.getNamespace(it) }
        val builder = SchemaBuilder.record(stdName)
        if (stdName != fieldName) {
            LOGGER.warn(
                "Schema name \"{}\" contains illegal character(s) and is standardized to \"{}\"",
                fieldName,
                stdName
            )
            builder.doc(
                String.format(
                    "%s",
                    fieldName
                )
            )
        }
        if (stdNamespace != null) {
            builder.namespace(stdNamespace)
        }
        val properties = Optional.ofNullable(
            jsonSchema["properties"]
        )
        // object field with no "properties" will be handled by the default additional properties
        // field during object conversion; so it is fine if there is no "properties"
        val subfieldNames = properties
            .map { p: JsonNode -> p.fieldNames().asSequence().toList() }
            .orElseGet { listOf() }
        val assembler = builder.fields()
        for (subfieldName in subfieldNames) {
            val stdFieldName = AvroConstants.NAME_TRANSFORMER.getIdentifier(subfieldName)
            val subfieldDefinition = properties.map { p: JsonNode -> p[subfieldName] }
                .orElseGet { JsonNodeFactory.instance.missingNode() }
            val fieldBuilder = assembler.name(stdFieldName)
                LOGGER.warn(
                    "Field name \"{}\" contains illegal character(s) and is standardized to \"{}\"",
                    subfieldName, stdFieldName
                )
                fieldBuilder.doc(
                    String.format(
                        "%s",
                        subfieldName
                    )
                )
            val subfieldNamespace = if (isRootNode // Omit the namespace for root level fields,
            // because it is directly assigned in the builder above.
            // This may not be the correct choice.
            ) null else if (stdNamespace == null) stdName else "$stdNamespace.$stdName"
            fieldBuilder.type(
                parseJsonField(
                    subfieldName,
                    subfieldNamespace,
                    subfieldDefinition,
                    appendExtraProps,
                    addStringToLogicalTypes
                )
            )
                .withDefault(null)
        }
        return assembler.endRecord()
    }

    /**
     * Generate Avro schema for a single Json field type. For example:
     *
     * <pre>
     * "number" -> ["double"]
    </pre> *
     */
    private fun parseSingleType(
        fieldName: String,
        fieldNamespace: String?,
        fieldType: JsonSchemaType,
        fieldDefinition: JsonNode,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean
    ): Schema {
        Preconditions.checkState(
            fieldType != JsonSchemaType.NULL,
            "Null types should have been filtered out"
        )
        val fieldSchema: Schema = when (fieldType) {
            JsonSchemaType.INTEGER, JsonSchemaType.LONG, JsonSchemaType.NUMBER, JsonSchemaType.BOOLEAN -> Schema.create(
                fieldType.avroType
            )

            JsonSchemaType.STRING -> getStringSchema(
                fieldType,
                fieldDefinition
            )

            JsonSchemaType.COMBINED -> getCombinedSchema(
                fieldName,
                fieldNamespace,
                fieldDefinition,
                appendExtraProps,
                addStringToLogicalTypes
            )

            JsonSchemaType.ARRAY -> getArraySchema(
                fieldName,
                fieldNamespace,
                fieldDefinition,
                appendExtraProps,
                addStringToLogicalTypes
            )

            JsonSchemaType.OBJECT -> getAvroSchema(
                fieldDefinition,
                fieldName,
                fieldNamespace,
                appendExtraProps,
                addStringToLogicalTypes,
                false
            )

            else -> {
                LOGGER.warn(
                    "Field \"{}\" has invalid type definition: {}. It will default to string.",
                    fieldName,
                    fieldDefinition
                )
                Schema.createUnion(
                    NULL_SCHEMA,
                    STRING_SCHEMA
                )
            }
        }
        return fieldSchema
    }

    private fun getArraySchema(
        fieldName: String,
        fieldNamespace: String?,
        fieldDefinition: JsonNode,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean
    ): Schema {
        val fieldSchema: Schema
        val items = fieldDefinition["items"]
        fieldSchema = if (items == null) {
            LOGGER.warn(
                "Array field \"{}\" does not specify the items type. It will default to an array of strings",
                fieldName
            )
            Schema.createArray(
                Schema.createUnion(
                    NULL_SCHEMA,
                    STRING_SCHEMA
                )
            )
        } else if (items.isObject) {
            if (!items.has("type") || items["type"].isNull) {
                LOGGER.warn(
                    "Array field \"{}\" does not specify the items type. it will default to an array of strings",
                    fieldName
                )
                Schema.createArray(
                    Schema.createUnion(
                        NULL_SCHEMA,
                        STRING_SCHEMA
                    )
                )
            } else {
                // Objects inside Json array has no names. We name it with the ".items" suffix.
                val elementFieldName = "$fieldName.items"
                Schema.createArray(
                    parseJsonField(
                        elementFieldName,
                        fieldNamespace, items,
                        appendExtraProps,
                        addStringToLogicalTypes
                    )
                )
            }
        } else if (items.isArray) {
            val arrayElementTypes = parseJsonTypeUnion(
                fieldName,
                fieldNamespace, items as ArrayNode,
                appendExtraProps,
                addStringToLogicalTypes
            )
            arrayElementTypes.add(0, NULL_SCHEMA)
            Schema.createArray(Schema.createUnion(arrayElementTypes))
        } else {
            LOGGER.warn(
                "Array field \"{}\" has invalid items specification: {}. It will default to an array of strings.",
                fieldName, items
            )
            Schema.createArray(
                Schema.createUnion(
                    NULL_SCHEMA,
                    STRING_SCHEMA
                )
            )
        }
        return fieldSchema
    }

    private fun getCombinedSchema(
        fieldName: String,
        fieldNamespace: String?,
        fieldDefinition: JsonNode,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean
    ): Schema {
        val fieldSchema: Schema
        val combinedRestriction = getCombinedRestriction(fieldDefinition)
        val unionTypes = parseJsonTypeUnion(
            fieldName,
            fieldNamespace,
            combinedRestriction.orElseGet { JsonNodeFactory.instance.arrayNode() } as ArrayNode,
            appendExtraProps,
            addStringToLogicalTypes)
        fieldSchema = Schema.createUnion(unionTypes)
        return fieldSchema
    }

    /**
     * Take in a union of Json field definitions, and generate Avro field schema unions. For example:
     *
     * <pre>
     * ["number", { ... }] -> ["double", { ... }]
    </pre> *
     */
    private fun parseJsonTypeUnion(
        fieldName: String,
        fieldNamespace: String?,
        types: ArrayNode,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean
    ): MutableList<Schema> {
        val schemas = types.elements().asSequence()
            .flatMap { definition: JsonNode ->
                getNonNullTypes(fieldName, definition).flatMap inner@{ type: JsonSchemaType ->
                    val namespace =
                        if (fieldNamespace == null) fieldName else "$fieldNamespace.$fieldName"
                    val singleFieldSchema = parseSingleType(
                        fieldName,
                        namespace,
                        type,
                        definition,
                        appendExtraProps,
                        addStringToLogicalTypes
                    )
                    return@inner if (singleFieldSchema.isUnion) {
                        singleFieldSchema.types.asSequence()
                    } else {
                        sequenceOf(singleFieldSchema)
                    }
                }
            }
            .distinct()
            .toList()
        return mergeRecordSchemas(fieldName, fieldNamespace, schemas)
    }

    /**
     * If there are multiple object fields, those fields are combined into one Avro record. This is
     * because Avro does not allow specifying a tuple of types (i.e. the first element is type x, the
     * second element is type y, and so on). For example, the following Json field types:
     *
     * <pre>
     * [
     * {
     * "type": "object",
     * "properties": {
     * "id": { "type": "integer" }
     * }
     * },
     * {
     * "type": "object",
     * "properties": {
     * "id": { "type": "string" }
     * "message": { "type": "string" }
     * }
     * }
     * ]
    </pre> *
     *
     * is converted to this Avro schema:
     *
     * <pre>
     * {
     * "type": "record",
     * "fields": [
     * { "name": "id", "type": ["int", "string"] },
     * { "name": "message", "type": "string" }
     * ]
     * }
    </pre> *
     */
    private fun mergeRecordSchemas(
        fieldName: String?,
        fieldNamespace: String?,
        schemas: List<Schema>
    ): MutableList<Schema> {
        val recordFieldSchemas = LinkedHashMap<String, MutableList<Schema>>()
        val recordFieldDocs: MutableMap<String, MutableList<String>> = HashMap()
        val mergedSchemas =
            schemas.toList() // gather record schemas to construct a single record schema later on
                .map { schema: Schema ->
                    if (schema.type == Schema.Type.RECORD) {
                        for (field in schema.fields) {
                            recordFieldSchemas.putIfAbsent(field.name(), mutableListOf())
                            recordFieldSchemas[field.name()]!!.add(field.schema())
                            if (field.doc() != null) {
                                recordFieldDocs.putIfAbsent(field.name(), mutableListOf())
                                recordFieldDocs[field.name()]!!.add(field.doc())
                            }
                        }
                    }
                    schema
                } // remove record schemas because they will be merged into one
                .filter { schema: Schema -> schema.type != Schema.Type.RECORD }
                .toMutableList()

        // create one record schema from all the record fields
        if (recordFieldSchemas.isNotEmpty()) {
            val builder = SchemaBuilder.record(fieldName)
            if (fieldNamespace != null) {
                builder.namespace(fieldNamespace)
            }
            val assembler = builder.fields()
            for ((subfieldName, value) in recordFieldSchemas) {
                val subfieldBuilder = assembler.name(subfieldName)
                val subfieldDocs = recordFieldDocs.getOrDefault(subfieldName, listOf())
                if (subfieldDocs.isNotEmpty()) {
                    subfieldBuilder.doc(java.lang.String.join("; ", subfieldDocs))
                }
                val subfieldSchemas = value.toList()
                    .flatMap { schema: Schema ->
                        schema.types.toList() // filter out null and add it later on as the first element
                            .filter { s: Schema -> s != NULL_SCHEMA }
                    }
                    .distinct()
                // recursively merge schemas of a subfield
                // because they may include multiple record schemas as well
                val mergedSubfieldSchemas =
                    mergeRecordSchemas(subfieldName, fieldNamespace, subfieldSchemas)
                mergedSubfieldSchemas.add(0, NULL_SCHEMA)
                subfieldBuilder.type(Schema.createUnion(mergedSubfieldSchemas)).withDefault(null)
            }
            mergedSchemas.add(assembler.endRecord())
        }
        return mergedSchemas
    }

    /**
     * Take in a Json field definition, and generate a nullable Avro field schema. For example:
     *
     * <pre>
     * {"type": ["number", { ... }]} -> ["null", "double", { ... }]
    </pre> *
     */
    private fun parseJsonField(
        fieldName: String,
        fieldNamespace: String?,
        fieldDefinition: JsonNode,
        appendExtraProps: Boolean,
        addStringToLogicalTypes: Boolean
    ): Schema {
        // Filter out null types, which will be added back in the end.
        val nonNullFieldTypes = getNonNullTypes(fieldName, fieldDefinition)
            .flatMap { fieldType: JsonSchemaType ->
                val singleFieldSchema = parseSingleType(
                    fieldName,
                    fieldNamespace,
                    fieldType,
                    fieldDefinition,
                    appendExtraProps,
                    addStringToLogicalTypes
                )
                if (singleFieldSchema.isUnion) {
                    return@flatMap singleFieldSchema.types.toList()
                } else {
                    return@flatMap listOf(singleFieldSchema)
                }
            }
            .distinct()
            .toMutableList()

        return if (nonNullFieldTypes.isEmpty()) {
            Schema.create(Schema.Type.NULL)
        } else {
            // Mark every field as nullable to prevent missing value exceptions from Avro / Parquet.
            if (!nonNullFieldTypes.contains(NULL_SCHEMA)) {
                nonNullFieldTypes.add(0, NULL_SCHEMA)
            }
            // Logical types are converted to a union of
            // logical type itself and string. The purpose is to
            // default the logical type field to a string,
            // if the value of the logical type field is invalid and
            // cannot be properly processed.
            if (nonNullFieldTypes
                    .stream().anyMatch { schema: Schema -> schema.logicalType != null } &&
                !nonNullFieldTypes.contains(STRING_SCHEMA) && addStringToLogicalTypes
            ) {
                nonNullFieldTypes.add(STRING_SCHEMA)
            }
            Schema.createUnion(nonNullFieldTypes)
        }
    }

    companion object {
        private const val TYPE = "type"
        private val NULL_SCHEMA = Schema.create(Schema.Type.NULL)
        private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
        private val LOGGER = LoggerFactory.getLogger(
            JsonToAvroSchemaConverter::class.java
        )

        fun getNonNullTypes(
            fieldName: String?,
            fieldDefinition: JsonNode
        ): List<JsonSchemaType> {
            return getTypes(fieldName, fieldDefinition)
                .filter { type: JsonSchemaType -> type != JsonSchemaType.NULL }
        }

        /**
         * When no type is specified, it will default to string.
         */
        private fun getTypes(fieldName: String?, fieldDefinition: JsonNode): List<JsonSchemaType> {
            val combinedRestriction = getCombinedRestriction(fieldDefinition)
            if (combinedRestriction.isPresent) {
                return listOf(JsonSchemaType.COMBINED)
            }
            val typeProperty = fieldDefinition[TYPE]
            if (typeProperty == null || typeProperty.isNull) {
                LOGGER.warn(
                    "Field \"{}\" has no type specification. It will default to string",
                    fieldName
                )
                return listOf(JsonSchemaType.STRING)
            }
            if (typeProperty.isArray) {
                return typeProperty.elements().asSequence()
                    .map { s: JsonNode -> JsonSchemaType.fromJsonSchemaTypeName(s.asText()) }
                    .toList()
            }
            if (typeProperty.isTextual) {
                return listOf(JsonSchemaType.fromJsonSchemaTypeName(typeProperty.asText()))
            }
            LOGGER.warn(
                "Field \"{}\" has unexpected type {}. It will default to string.",
                fieldName,
                typeProperty
            )
            return listOf(JsonSchemaType.STRING)
        }

        fun getCombinedRestriction(fieldDefinition: JsonNode): Optional<JsonNode> {
            if (fieldDefinition.has("anyOf")) {
                return Optional.of(
                    fieldDefinition["anyOf"]
                )
            }
            if (fieldDefinition.has("allOf")) {
                return Optional.of(
                    fieldDefinition["allOf"]
                )
            }
            return if (fieldDefinition.has("oneOf")) {
                Optional.of(fieldDefinition["oneOf"])
            } else Optional.empty()
        }

        private fun getStringSchema(fieldType: JsonSchemaType, fieldDefinition: JsonNode): Schema {
            val fieldSchema: Schema = if (fieldDefinition.has("format")) {
                when (fieldDefinition["format"].asText()) {
                    "date-time" -> {
                        LogicalTypes.timestampMicros()
                            .addToSchema(Schema.create(Schema.Type.LONG))
                    }
                    "date" -> {
                        LogicalTypes.date()
                            .addToSchema(Schema.create(Schema.Type.INT))
                    }
                    "time" -> {
                        LogicalTypes.timeMicros()
                            .addToSchema(Schema.create(Schema.Type.LONG))
                    }
                    else -> {
                        Schema.create(fieldType.avroType)
                    }
                }
            } else {
                Schema.create(fieldType.avroType)
            }
            return fieldSchema
        }
    }
}
