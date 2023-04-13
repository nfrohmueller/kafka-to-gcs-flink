package de.rewe.kafkatogcs.kafka

import java.time.Instant

data class KafkaMessage(
    val id: String,
    val key: String,
    val time: Instant,
    val type: String,
    val payload: String
)
