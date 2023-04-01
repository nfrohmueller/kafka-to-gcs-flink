/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */
package de.rewe.kafkatogcs.schema.conversion.integrations

import de.rewe.kafkatogcs.schema.conversion.commons.Names

open class StandardNameTransformer : NamingConventionTransformer {
    override fun getIdentifier(name: String): String {
        return convertStreamName(name)
    }

    /**
     * Most destinations have the same naming requirement for namespace and stream names.
     */
    override fun getNamespace(namespace: String): String {
        return convertStreamName(namespace)
    }

    override fun convertStreamName(input: String): String {
        return Names.toAlphanumericAndUnderscore(input)
    }
}
