package de.rewe.kafkatogcs

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.*
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.TableDescriptor

import org.apache.flink.table.api.Expressions.*
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import java.nio.file.Files
import java.nio.file.Paths


fun main() {
    val outputPath = Paths.get("file:///Users/nilsfrohmuller/projects/architecture/analytics/kafka-to-gcs-flink/output")
    Files.deleteIfExists(outputPath)

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
    streamEnv.checkpointConfig.checkpointingMode = CheckpointingMode.EXACTLY_ONCE
    streamEnv.checkpointConfig.checkpointInterval = 1000

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    val inputSchema = Schema.newBuilder()
        .column("id", STRING())
        .column("key", STRING())
        .column("time", TIMESTAMP_LTZ(3))
        .column("type", STRING())
        .column("payload", STRING())
        .watermark("time", col("time").minus(lit(5).seconds()))
        .build()

    val kafkaSource = TableDescriptor.forConnector("kafka")
        .schema(inputSchema)
        .option("topic", "market_v1")
        .option("properties.bootstrap.servers", "bootstrap.bd-int.kafka.rewe.cloud:9094")
        .option("scan.startup.mode", "earliest-offset")
        .option("value.format", "json")
        .option("value.json.timestamp-format.standard", "ISO-8601")
        .build()
    val kafkaSourceTableName = "kafka_source"
    tableEnv.createTable(kafkaSourceTableName, kafkaSource)

    val outputSchema = Schema.newBuilder()
        .fromSchema(inputSchema)
        .column("dt", STRING())
        .column("hour", STRING())
        .build()

    val kafkaPrint = TableDescriptor.forConnector("print")
        .schema(outputSchema)
        .build()
    val printTableName = "print_table"
    tableEnv.createTable(printTableName, kafkaPrint)


    val fileSink = TableDescriptor.forConnector("filesystem")
        .schema(outputSchema)
        .option("path", outputPath.toString())
        .option("format", "json")
        .option("partition.time-extractor.timestamp-pattern", "\$dt \$hour:00:00")
        .option("sink.partition-commit.delay", "1 m")
        .option("sink.partition-commit.trigger", "partition-time")
        .option("sink.partition-commit.policy.kind", "success-file")
        .option("sink.partition-commit.watermark-time-zone", "UTC")
        .option("sink.rolling-policy.rollover-interval", "1 m")
        .partitionedBy("dt", "hour")
        .build()
    val fileSinkTableName = "file_sink"
    tableEnv.createTable(fileSinkTableName, fileSink)

    val table = tableEnv
        .from(kafkaSourceTableName)
        .addColumns(dateFormat(col("time"), "yyyy-MM-dd").`as`("dt"))
        .addColumns(dateFormat(col("time"), "HH").`as`("hour"))

    val transformed = tableEnv.fromDataStream(tableEnv.toDataStream(table).map { row: Row -> row })

//    table.executeInsert(printTableName)
    transformed.executeInsert(fileSink)
}

