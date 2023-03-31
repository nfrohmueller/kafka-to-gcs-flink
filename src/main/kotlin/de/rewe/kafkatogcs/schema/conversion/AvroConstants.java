package de.rewe.kafkatogcs.schema.conversion;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * {@link AvroConstants} holds constant refs to an {@link AvroNameTransformer}
 * and a {@link JsonAvroConverter}.
 */
public class AvroConstants {

  private AvroConstants() {}

  public static final AvroNameTransformer NAME_TRANSFORMER = new AvroNameTransformer();
  public static final JsonAvroConverter JSON_CONVERTER = JsonAvroConverter.builder()
      .setNameTransformer(NAME_TRANSFORMER::getIdentifier)
      .build();

}
