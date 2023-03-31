/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package de.rewe.kafkatogcs.schema.conversion.integrations;


import de.rewe.kafkatogcs.schema.conversion.commons.Names;

public class StandardNameTransformer implements NamingConventionTransformer {

  @Override
  public String getIdentifier(final String name) {
    return convertStreamName(name);
  }

  /**
   * Most destinations have the same naming requirement for namespace and stream names.
   */
  @Override
  public String getNamespace(final String namespace) {
    return convertStreamName(namespace);
  }

  @Override
  public String convertStreamName(final String input) {
    return Names.toAlphanumericAndUnderscore(input);
  }

}
