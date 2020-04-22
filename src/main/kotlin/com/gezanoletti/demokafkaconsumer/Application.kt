package com.gezanoletti.demokafkaconsumer

import com.gezanoletti.demokafkaconsumer.KafkaConstants.MAX_POLL_RECORDS
import com.gezanoletti.demokafkaconsumer.KafkaConstants.OFFSET_RESET_EARLIER
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_COMPACT
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_NORMAL
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.lang.Thread.sleep
import java.time.Duration

fun main() {
    val props = mapOf(
        BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
        GROUP_ID_CONFIG to KafkaConstants.GROUP_ID_CONFIG,
        KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        MAX_POLL_RECORDS_CONFIG to MAX_POLL_RECORDS,
        ENABLE_AUTO_COMMIT_CONFIG to "false",
        AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET_EARLIER
    )

    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf(TOPIC_NAME_NORMAL, TOPIC_NAME_COMPACT))

    try {
        while (true) {
            sleep(500)
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(10))

            records.forEach {
                println("Record: key ${it.key()}, value ${it.value()}, partition ${it.partition()}, offset ${it.offset()}")
            }

            consumer.commitAsync()
        }

    } catch (e: Exception) {
        println(e.message)
    } finally {
        consumer.close()
    }

}

object KafkaConstants {
    const val KAFKA_BROKERS = "localhost:9092"
    const val GROUP_ID_CONFIG = "consumerGroup3"
    const val TOPIC_NAME_NORMAL = "demo-basic-kafka-partitions"
    const val TOPIC_NAME_COMPACT = "demo-basic-kafka-partitions-compact"
//    const val OFFSET_RESET_LATEST = "latest"
    const val OFFSET_RESET_EARLIER = "earliest"
    const val MAX_POLL_RECORDS = 10
}
