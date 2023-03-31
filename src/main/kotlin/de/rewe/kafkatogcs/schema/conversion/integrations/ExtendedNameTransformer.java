/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package de.rewe.kafkatogcs.schema.conversion.integrations;

/**
 * When choosing identifiers names in destinations, extended Names can handle more special
 * characters than standard Names by using the quoting characters: "..."
 * This class detects when such special characters are used and adds the appropriate quoting when
 * necessary.
 */
public class ExtendedNameTransformer extends StandardNameTransformer {

  @Override
  public String convertStreamName(final String input) {
    return super.convertStreamName(input);
  }

}
