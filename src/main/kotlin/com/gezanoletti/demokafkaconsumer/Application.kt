package com.gezanoletti.demokafkaconsumer

import com.gezanoletti.demokafkaconsumer.KafkaConstants.MAX_POLL_RECORDS
import com.gezanoletti.demokafkaconsumer.KafkaConstants.OFFSET_RESET_EARLIER
import com.gezanoletti.demokafkaconsumer.KafkaConstants.SCHEMA_REGISTRY_URL
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_AVRO
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_AVRO_STREAM
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_COMPACT
import com.gezanoletti.demokafkaconsumer.KafkaConstants.TOPIC_NAME_NORMAL
import com.gezanoletti.demokafkaproducer.avro.PersonMessageKey
import com.gezanoletti.demokafkaproducer.avro.PersonMessageValue
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.lang.Thread.sleep
import java.time.Duration

fun main() {
    val consumer1 = KafkaConsumer<String, String>(
        mapOf(
            BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
            GROUP_ID_CONFIG to KafkaConstants.GROUP_ID_CONFIG,
            KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            MAX_POLL_RECORDS_CONFIG to MAX_POLL_RECORDS,
            ENABLE_AUTO_COMMIT_CONFIG to "false",
            AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET_EARLIER
        )
    )
    consumer1.subscribe(listOf(TOPIC_NAME_NORMAL, TOPIC_NAME_COMPACT))

    val consumer2 = KafkaConsumer<PersonMessageKey, PersonMessageValue>(
        mapOf(
            BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
            GROUP_ID_CONFIG to KafkaConstants.GROUP_ID_CONFIG,
            KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            MAX_POLL_RECORDS_CONFIG to MAX_POLL_RECORDS,
            ENABLE_AUTO_COMMIT_CONFIG to "false",
            AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET_EARLIER,
            SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL,
            SPECIFIC_AVRO_READER_CONFIG to true
        )
    )
    consumer2.subscribe(listOf(TOPIC_NAME_AVRO, TOPIC_NAME_AVRO_STREAM))

    try {
        while (true) {
            val records1 = consumer1.poll(Duration.ofMillis(10))
            records1.forEach {
                println("Record: key=${it.key()}, value=${it.value()}, topic=${it.topic()}, partition=${it.partition()}, offset=${it.offset()}")
            }
            consumer1.commitAsync()

            val records2 = consumer2.poll(Duration.ofMillis(10))
            records2.forEach {
                println(
                    "Record: key=${it.key().getId()}, value=[${it.value().getName()} ${it.value()
                        .getSurname()}], topic=${it.topic()}, partition=${it.partition()}, offset=${it.offset()}"
                )
            }
            consumer2.commitAsync()

            sleep(500)
        }

    } catch (e: Exception) {
        println(e.message)
    } finally {
        consumer1.close()
    }

}

object KafkaConstants {
    const val KAFKA_BROKERS = "localhost:9092"
    const val GROUP_ID_CONFIG = "consumerGroup3"
    const val TOPIC_NAME_NORMAL = "demo-basic-kafka-partitions"
    const val TOPIC_NAME_COMPACT = "demo-basic-kafka-partitions-compact"
    const val TOPIC_NAME_AVRO = "demo-basic-kafka-partitions-avro"
    const val TOPIC_NAME_AVRO_STREAM = "demo-basic-kafka-partitions-avro-stream"
    //    const val OFFSET_RESET_LATEST = "latest"
    const val OFFSET_RESET_EARLIER = "earliest"
    const val MAX_POLL_RECORDS = 10
    const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
}
