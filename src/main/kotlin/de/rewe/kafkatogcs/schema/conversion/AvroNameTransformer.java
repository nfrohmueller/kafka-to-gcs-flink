package de.rewe.kafkatogcs.schema.conversion;

import de.rewe.kafkatogcs.schema.conversion.integrations.ExtendedNameTransformer;
import java.util.Arrays;
import java.util.stream.Collectors;


/**
 * {@link AvroNameTransformer} Utility class for correctly handling names in avro.
 *
 * <ul>
 * <li>An Avro name starts with [A-Za-z_], followed by [A-Za-z0-9_].</li>
 * <li>An Avro namespace is a dot-separated sequence of such names.</li>
 * <li>Reference: <a href="https://avro.apache.org/docs/current/spec.html#names">Avro Specification</a></li>
 * </ul>
 */
public class AvroNameTransformer extends ExtendedNameTransformer {

  @Override
  public String convertStreamName(final String input) {
    if (input == null) {
      return null;
    } else if (input.isBlank()) {
      return input;
    }

    final String normalizedName = super.convertStreamName(input);
    if (normalizedName.substring(0, 1).matches("[A-Za-z_]")) {
      return normalizedName;
    } else {
      return "_" + normalizedName;
    }
  }

  @Override
  public String getNamespace(final String input) {
    if (input == null) {
      return null;
    }

    final String[] tokens = input.split("\\.");
    return Arrays.stream(tokens).map(this::getIdentifier).collect(Collectors.joining("."));
  }

}
