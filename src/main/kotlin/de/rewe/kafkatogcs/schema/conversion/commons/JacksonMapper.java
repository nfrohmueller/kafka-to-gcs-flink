package de.rewe.kafkatogcs.schema.conversion.commons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The {@link JavaTimeModule} allows mappers to accommodate different varieties of serialised date
 * time strings.
 * <p>
 * All jackson mapper creation should use the following methods for instantiation.
 */
public class JacksonMapper {

  private JacksonMapper() {}

  /**
   * Create the standard ObjectMapper correctly initialized with the required modules.
   *
   * @return {@link ObjectMapper} the default ObjectMapper
   */
  public static ObjectMapper initMapper() {
    final ObjectMapper result = new ObjectMapper().registerModule(new JavaTimeModule());
    result.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return result;
  }
}
