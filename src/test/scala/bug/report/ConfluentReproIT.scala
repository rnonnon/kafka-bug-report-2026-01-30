package bug.report

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}
import org.apache.kafka.common.errors.RecordTooLargeException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Properties
import java.util.concurrent.{ExecutionException, Future}
import scala.jdk.CollectionConverters._

class ConfluentReproIT extends AnyFunSpec with Matchers with ScalaFutures {

  private val logger = LoggerFactory.getLogger(classOf[ConfluentReproIT])

  describe("Kafka producer batching behavior") {
    it(
      "should fail all messages in a batch if one is too large for the broker"
    ) {
      val testTimestamp = System.currentTimeMillis()
      val topicName = s"confluent-repro-topic-${testTimestamp}"
      val messageMaxBytes = 200 * 1024 // 200KB

      // Create topic with max.message.bytes
      createTopic(
        topicName,
        partitions = 1,
        replicationFactor = 1,
        Map("max.message.bytes" -> messageMaxBytes.toString)
      )

      val props = new Properties()
      props.putAll(
        Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ->
            "localhost:9092",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ->
            "org.apache.kafka.common.serialization.StringSerializer",
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ->
            "org.apache.kafka.common.serialization.ByteArraySerializer",
          ProducerConfig.BATCH_SIZE_CONFIG ->
            (300 * 1024).toString, // batch size larger than message.max.bytes
          ProducerConfig.RETRIES_CONFIG -> "0",
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "false",
          ProducerConfig.LINGER_MS_CONFIG -> "10000" // linger to ensure batching
        ).asJava
      )

      val producer = new KafkaProducer[String, Array[Byte]](props)

      val smallMessage = new Array[Byte](10 * 1024) // 10KB
      val largeMessage =
        new Array[Byte](250 * 1024) // 250KB > 200KB (message.max.bytes)

      val smallMessageRecord =
        new ProducerRecord[String, Array[Byte]](topicName, "key1", smallMessage)
      val largeMessageRecord =
        new ProducerRecord[String, Array[Byte]](topicName, "key2", largeMessage)

      // Send both messages. Due to linger.ms and batch.size, they should be in the same batch.
      logger.debug("Sending small message...")
      val futureSmall: Future[RecordMetadata] =
        producer.send(smallMessageRecord)
      logger.debug("Sending large message...")
      val futureLarge: Future[RecordMetadata] =
        producer.send(largeMessageRecord)
      logger.debug("Flushing producer...")
      producer.flush()
      logger.debug("Producer flushed.")

      // The large message should fail because it's larger than the topic's message.max.bytes
      val exLarge = intercept[ExecutionException] {
        futureLarge.get()
      }
      logger.debug("Large message failed as expected", exLarge)
      exLarge.getCause shouldBe a[RecordTooLargeException]

      // The small message should also fail because it's part of the same batch.
      val exSmall = intercept[ExecutionException] {
        futureSmall.get()
      }
      logger.debug("Small message failed as expected", exSmall)
      exSmall.getCause shouldBe a[RecordTooLargeException]

      producer.close()
    }
  }

  private def createTopic(
      name: String,
      partitions: Int,
      replicationFactor: Int,
      config: Map[String, String]
  ): Unit = {
    val newTopic = new org.apache.kafka.clients.admin.NewTopic(
      name,
      partitions,
      replicationFactor.toShort
    )
    newTopic.configs(config.asJava)
    val adminClient = getAdminClient
    try
      adminClient.createTopics(List(newTopic).asJava).all().get()
    finally
      adminClient.close()
  }

  private def getAdminClient = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    org.apache.kafka.clients.admin.AdminClient.create(props)
  }
}
