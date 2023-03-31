/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package de.rewe.kafkatogcs.schema.conversion.integrations;

/**
 * Destination have their own Naming conventions (which characters are valid or rejected in
 * identifiers names) This class transform a random string used to a valid identifier names for each
 * specific destination.
 */
public interface NamingConventionTransformer {

  /**
   * Handle Naming Conversions of an input name to output a valid identifier name for the desired
   * destination.
   *
   * @param name of the identifier to check proper naming conventions
   * @return modified name with invalid characters replaced by '_' and adapted for the chosen
   *         destination.
   */
  String getIdentifier(String name);

  /**
   * Handle naming conversions of an input name to output a valid namespace for the desired
   * destination.
   */
  String getNamespace(String namespace);

  String convertStreamName(final String input);


}
