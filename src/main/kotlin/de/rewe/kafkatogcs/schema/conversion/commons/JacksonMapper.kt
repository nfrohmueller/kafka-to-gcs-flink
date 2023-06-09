package de.rewe.kafkatogcs.schema.conversion.commons

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

/**
 * The [JavaTimeModule] allows mappers to accommodate different varieties of serialised date
 * time strings.
 *
 *
 * All jackson mapper creation should use the following methods for instantiation.
 */
val objectMapper: ObjectMapper by lazy {
    val result = ObjectMapper().registerModule(JavaTimeModule())
    result.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    result
}
