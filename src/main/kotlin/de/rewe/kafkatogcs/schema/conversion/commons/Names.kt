/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */
package de.rewe.kafkatogcs.schema.conversion.commons

import java.text.Normalizer

object Names {
    private const val NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN = "[^\\p{Alnum}_]"

    /**
     * Converts any UTF8 string to a string with only alphanumeric and _ characters without preserving
     * accent characters.
     *
     * @param s string to convert
     * @return cleaned string
     */
    fun toAlphanumericAndUnderscore(s: String?): String {
        return Normalizer.normalize(s, Normalizer.Form.NFKD)
            .replace(
                "\\p{M}".toRegex(),
                ""
            ) // P{M} matches a code point that is not a combining mark (unicode)
            .replace("\\s+".toRegex(), "_")
            .replace(NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN.toRegex(), "_")
    }
}
