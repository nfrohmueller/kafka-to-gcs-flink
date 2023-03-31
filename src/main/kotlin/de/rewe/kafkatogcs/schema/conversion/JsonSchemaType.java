/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package de.rewe.kafkatogcs.schema.conversion;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;

/**
 * Mapping of JsonSchema types to Avro types.
 */
public enum JsonSchemaType {

  STRING("string", true, Schema.Type.STRING),
  NUMBER("number", true, Schema.Type.DOUBLE),
  INTEGER("integer", true, Schema.Type.INT),
  LONG("long", true, Schema.Type.LONG),
  BOOLEAN("boolean", true, Schema.Type.BOOLEAN),
  NULL("null", true, Schema.Type.NULL),
  OBJECT("object", false, Schema.Type.RECORD),
  ARRAY("array", false, Schema.Type.ARRAY),
  COMBINED("combined", false, Schema.Type.UNION);

  private final String jsonSchemaTypeName;
  private final boolean isPrimitive;
  private final Schema.Type avroType;

  JsonSchemaType(
      final String jsonSchemaTypeName,
      final boolean isPrimitive,
      final Schema.Type avroType) {
    this.jsonSchemaTypeName = jsonSchemaTypeName;
    this.isPrimitive = isPrimitive;
    this.avroType = avroType;
  }

  /**
   * Derive {@link JsonSchemaType} from it's name.
   *
   * @param jsonSchemaTypeName The name
   * @return {@link JsonSchemaType}
   */
  public static JsonSchemaType fromJsonSchemaTypeName(final String jsonSchemaTypeName) {
    // Match by Type are no results already
    List<JsonSchemaType> matchSchemaType = Arrays.stream(values())
        .filter(format -> jsonSchemaTypeName.equals(format.jsonSchemaTypeName))
        .collect(toList());

    if (matchSchemaType.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Unexpected jsonSchemaType - %s", jsonSchemaTypeName));
    } else if (matchSchemaType.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Match with more than one json type! Matched types : %s, Inputs jsonSchemaType : %s",
              matchSchemaType,
              jsonSchemaTypeName));
    } else {
      return matchSchemaType.get(0);
    }
  }

  public boolean isPrimitive() {
    return isPrimitive;
  }

  public Schema.Type getAvroType() {
    return avroType;
  }

  @Override
  public String toString() {
    return jsonSchemaTypeName;
  }

}
