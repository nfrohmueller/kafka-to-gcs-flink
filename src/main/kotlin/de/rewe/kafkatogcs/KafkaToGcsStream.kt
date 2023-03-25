package de.rewe.kafkatogcs

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema
import org.apache.flink.formats.json.JsonRowSchemaConverter
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import java.io.File
import java.time.Duration

fun main() {

    val outputPath =
        Path("file:///Users/nilsfrohmuller/projects/architecture/analytics/kafka-to-gcs-flink/output")
    val jsonSchemaString = File("/Users/nilsfrohmuller/projects/architecture/analytics/kafka-to-gcs-flink/src/test/resources/market_v1.json").readText(Charsets.UTF_8)
    val typeInformation = JsonRowSchemaConverter.convert<String>(jsonSchemaString)

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
    streamEnv.checkpointConfig.checkpointingMode = CheckpointingMode.EXACTLY_ONCE
    streamEnv.checkpointConfig.checkpointInterval = 1000L

    val kafkaSource = KafkaSource.builder<String>()
        .setBootstrapServers("bootstrap.bd-int.kafka.rewe.cloud:9094")
        .setTopics("market_v1")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()
    val source =
        streamEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")


    val fileSink = FileSink
        .forRowFormat(outputPath, SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofSeconds(10))
                .withInactivityInterval(Duration.ofSeconds(5))
                .build()
        ).build()

    source.sinkTo(fileSink)
//    source.print()
    streamEnv.execute("Kafka Job")
}
