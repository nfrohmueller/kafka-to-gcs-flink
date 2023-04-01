package de.rewe.kafkatogcs.schema.conversion

import tech.allegro.schema.json2avro.converter.JsonAvroConverter

/**
 * [AvroConstants] holds constant refs to an [AvroNameTransformer]
 * and a [JsonAvroConverter].
 */
val NAME_TRANSFORMER = AvroNameTransformer()
val JSON_CONVERTER = JsonAvroConverter()
