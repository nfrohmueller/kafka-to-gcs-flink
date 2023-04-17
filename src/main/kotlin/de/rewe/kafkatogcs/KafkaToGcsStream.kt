package de.rewe.kafkatogcs

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addEnvironmentSource
import com.sksamuel.hoplite.addResourceSource
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.subject.TopicNameStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.slf4j.LoggerFactory
import java.time.Duration

val log = LoggerFactory.getLogger("KafkaToGcsStream")

fun main() {
    println("Starting application")


    val config = ConfigLoaderBuilder.default()
        .addEnvironmentSource()
        .addResourceSource("/application.yaml")
        .build()
        .loadConfigOrThrow<ApplicationConfig>()
    val subject = TopicNameStrategy().subjectName(config.kafka.topic, false, null)

    val outputPath =
        Path("file:///Users/nilsfrohmuller/projects/architecture/analytics/kafka-to-gcs-flink/output")

    val schemaRegistry = CachedSchemaRegistryClient(config.kafka.schemaRegistry, 1000)
    val latestSchemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject)
    val schema = Schema.Parser().parse(latestSchemaMetadata.schema)

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
    streamEnv.checkpointConfig.checkpointingMode = CheckpointingMode.EXACTLY_ONCE
    streamEnv.checkpointConfig.checkpointInterval = 1000L

    val kafkaSource = KafkaSource.builder<GenericRecord>()
        .setBootstrapServers(config.kafka.bootstrapServers)
        .setTopics(config.kafka.topic)
        .setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
        .setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        .setProperty(SaslConfigs.SASL_JAAS_CONFIG, """org.apache.kafka.common.security.plain.PlainLoginModule required username="${config.kafka.username}" password="${config.kafka.password}";""")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, config.kafka.schemaRegistry))
//        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()
    val source: DataStream<String> = streamEnv
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
        .map { gr: GenericRecord ->
            val str = gr.toString()
            println(str)
            return@map str
        }


    val fileSink = FileSink
        .forRowFormat(outputPath, SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofSeconds(10))
                .withInactivityInterval(Duration.ofSeconds(5))
                .build()
        ).build()

    source.sinkTo(fileSink)
    println("Executing pipeline")
//    source.print()
    streamEnv.execute(config.applicationName)
}

data class ApplicationConfig (
    val applicationName: String,
    val kafka: KafkaConfig
)

data class KafkaConfig(
    val bootstrapServers: String,
    val topic: String,
    val schemaRegistry: String,
    val username: String,
    val password: String
)
