package de.rewe.kafkatogcs.schema.conversion

import de.rewe.kafkatogcs.schema.conversion.integrations.ExtendedNameTransformer

/**
 * [AvroNameTransformer] Utility class for correctly handling names in avro.
 *
 *
 *  * An Avro name starts with [A-Za-z_], followed by [A-Za-z0-9_].
 *  * An Avro namespace is a dot-separated sequence of such names.
 *  * Reference: [Avro Specification](https://avro.apache.org/docs/current/spec.html#names)
 *
 */
class AvroNameTransformer : ExtendedNameTransformer() {
    override fun convertStreamName(input: String): String {
        if (input.isBlank()) {
            return input
        }
        val normalizedName = super.convertStreamName(input)
        return if (normalizedName.substring(0, 1).matches("[A-Za-z_]".toRegex())) {
            normalizedName
        } else {
            "_$normalizedName"
        }
    }

    override fun getNamespace(namespace: String): String {
        val tokens = namespace.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        return tokens.map { name: String? ->
            if (name != null) {
                getIdentifier(name)
            }
        }.joinToString(".")
    }
}
