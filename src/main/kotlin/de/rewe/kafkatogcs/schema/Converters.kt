package de.rewe.kafkatogcs.schema

import de.rewe.kafkatogcs.schema.JsonTypes.*
import de.rewe.kafkatogcs.schema.conversion.JsonToAvroSchemaConverter
import de.rewe.kafkatogcs.schema.conversion.commons.objectMapper
import org.apache.flink.table.api.DataTypes.*
import org.apache.flink.table.api.Schema
import org.apache.flink.table.types.DataType



fun convertVertXtoFlinkSchema(vertxSchema: net.jimblackler.jsonschemafriend.Schema): Schema {
    val schemaBuilder = Schema.newBuilder()


    return schemaBuilder.build()
}

fun convertSchemaToFlinkType(schema: net.jimblackler.jsonschemafriend.Schema): DataType {
    val type: String = schema.explicitTypes.first().uppercase()

    val resType = when (JsonTypes.valueOf(type)) {
        NULL -> NULL()
        BOOLEAN -> BOOLEAN()
        OBJECT -> resolveRow(schema)
        ARRAY -> ARRAY(if(schema.items != null) convertSchemaToFlinkType(schema.items) else STRING())
        NUMBER -> DOUBLE()
        STRING -> STRING()
        INTEGER -> INT()
    }

    return resType
}

fun resolveRow(schema: net.jimblackler.jsonschemafriend.Schema): DataType {
    val properties = schema.properties
    val fields = properties.asSequence().map { FIELD(it.key, convertSchemaToFlinkType(it.value)) }.toList()
    return ROW(fields)
}

enum class JsonTypes {
    NULL, BOOLEAN, OBJECT, ARRAY, NUMBER, STRING, INTEGER
}
