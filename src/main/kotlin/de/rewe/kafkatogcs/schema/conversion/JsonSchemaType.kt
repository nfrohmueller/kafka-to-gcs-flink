/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */
package de.rewe.kafkatogcs.schema.conversion

import org.apache.avro.Schema

/**
 * Mapping of JsonSchema types to Avro types.
 */
enum class JsonSchemaType(
    private val jsonSchemaTypeName: String,
    val isPrimitive: Boolean,
    val avroType: Schema.Type
) {
    STRING("string", true, Schema.Type.STRING),
    NUMBER("number", true, Schema.Type.DOUBLE),
    INTEGER("integer", true, Schema.Type.INT),
    LONG("long", true, Schema.Type.LONG),
    BOOLEAN("boolean", true, Schema.Type.BOOLEAN),
    NULL("null", true, Schema.Type.NULL),
    OBJECT("object", false, Schema.Type.RECORD),
    ARRAY("array", false, Schema.Type.ARRAY),
    COMBINED("combined", false, Schema.Type.UNION);

    override fun toString(): String {
        return jsonSchemaTypeName
    }

    companion object {
        /**
         * Derive [JsonSchemaType] from it's name.
         *
         * @param jsonSchemaTypeName The name
         * @return [JsonSchemaType]
         */
        fun fromJsonSchemaTypeName(jsonSchemaTypeName: String): JsonSchemaType {
            // Match by Type are no results already
            val matchSchemaType = values().asSequence()
                .filter { format: JsonSchemaType -> jsonSchemaTypeName == format.jsonSchemaTypeName }
                .toList()
            return if (matchSchemaType.isEmpty()) {
                throw IllegalArgumentException(
                    String.format(
                        "Unexpected jsonSchemaType - %s",
                        jsonSchemaTypeName
                    )
                )
            } else if (matchSchemaType.size > 1) {
                throw IllegalStateException(
                    String.format(
                        "Match with more than one json type! Matched types : %s, Inputs jsonSchemaType : %s",
                        matchSchemaType,
                        jsonSchemaTypeName
                    )
                )
            } else {
                matchSchemaType[0]
            }
        }
    }
}
