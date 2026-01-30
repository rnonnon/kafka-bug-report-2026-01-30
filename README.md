# Bug Report: Kafka Producer Enters Infinite Loop with Large Message in Batch

This report details a bug in the `kafka-clients` library where the producer enters an infinite loop when sending a batch of messages containing a message that is too large for the topic's configured `max.message.bytes`.

## Issue Summary

When a Kafka producer attempts to send a batch of messages where at least one message exceeds the broker's `max.message.bytes` limit, it enters an infinite loop of retries, even when `retries` is configured to `0`. This behavior appears to be caused by the producer's `Sender` attempting to split the oversized batch and resend it, without ever removing the message that is too large.

## Component/Version

- **Kafka-clients version:** `4.1.1` (as reported in the logs but also observed in 3.9.1 and all 4.X versions)
- **Confluent Platform version (for broker):** `7.6.1`

## Steps to Reproduce

1.  **Set up the environment:**
    - Start a Kafka broker using the provided `docker-compose.yml` file.

    ```yaml
    version: "3"
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.6.1
        hostname: zookeeper
        container_name: zookeeper
        ports:
          - "2181:2181"
        environment:
          ZOOKEEPER_CLIENT_PORT: 2181
          ZOOKEEPER_TICK_TIME: 2000

      broker:
        image: confluentinc/cp-kafka:7.6.1
        hostname: broker
        container_name: broker
        depends_on:
          - zookeeper
        ports:
          - "9092:9092"
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    ```

2.  **Project Configuration:**
    - Use the following `build.sbt` configuration for a Scala project.

    ```sbt
    ThisBuild / version := "0.1.0-SNAPSHOT"
    ThisBuild / scalaVersion := "2.13.12"

    lazy val root = (project in file("."))
      .settings(
        name := "kafka-bug-report",
        libraryDependencies += "org.apache.kafka" % "kafka-clients" % "4.1.1",
        libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test,
        libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.12" % Test
      )
    ```

3.  **Execute the Test Code:**
    - Run the following Scala test case. The test creates a topic with `max.message.bytes` set to 200KB, then sends a batch containing one small message (10KB) and one large message (250KB).

    ```scala
    package bug.report

    import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
    import org.apache.kafka.common.errors.RecordTooLargeException
    import org.scalatest.funspec.AnyFunSpec
    import org.scalatest.matchers.should.Matchers
    import org.slf4j.LoggerFactory
    import java.util.Properties
    import java.util.concurrent.{ExecutionException, Future}
    import scala.jdk.CollectionConverters._

    class ConfluentReproIT extends AnyFunSpec with Matchers {

      private val logger = LoggerFactory.getLogger(classOf[ConfluentReproIT])

      describe("Kafka producer batching behavior") {
        it("should fail all messages in a batch if one is too large for the broker") {
          val topicName = s"confluent-repro-topic-${System.currentTimeMillis()}"
          val messageMaxBytes = 200 * 1024 // 200KB

          createTopic(topicName, 1, 1, Map("max.message.bytes" -> messageMaxBytes.toString))

          val props = new Properties()
          props.putAll(
            Map(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
              ProducerConfig.BATCH_SIZE_CONFIG -> (300 * 1024).toString,
              ProducerConfig.RETRIES_CONFIG -> "0",
              ProducerConfig.LINGER_MS_CONFIG -> "10000"
            ).asJava
          )

          val producer = new KafkaProducer[String, Array[Byte]](props)
          val smallMessage = new Array[Byte](10 * 1024) // 10KB
          val largeMessage = new Array[Byte](250 * 1024) // 250KB > 200KB

          val smallMessageRecord = new ProducerRecord[String, Array[Byte]](topicName, "key1", smallMessage)
          val largeMessageRecord = new ProducerRecord[String, Array[Byte]](topicName, "key2", largeMessage)

          logger.debug("Sending small and large messages...")
          val futureSmall: Future[RecordMetadata] = producer.send(smallMessageRecord)
          val futureLarge: Future[RecordMetadata] = producer.send(largeMessageRecord)
          producer.flush()

          val exLarge = intercept[ExecutionException] { futureLarge.get() }
          exLarge.getCause shouldBe a[RecordTooLargeException]

          val exSmall = intercept[ExecutionException] { futureSmall.get() }
          exSmall.getCause shouldBe a[RecordTooLargeException]

          producer.close()
        }
      }

      private def createTopic(name: String, partitions: Int, replicationFactor: Int, config: Map[String, String]): Unit = {
        val newTopic = new org.apache.kafka.clients.admin.NewTopic(name, partitions, replicationFactor.toShort)
        newTopic.configs(config.asJava)
        val adminClient = {
          val props = new Properties()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
          org.apache.kafka.clients.admin.AdminClient.create(props)
        }
        try adminClient.createTopics(List(newTopic).asJava).all().get()
        finally adminClient.close()
      }
    }
    ```

## Expected Result

The producer should attempt to send the batch, and the broker should reject it with a `MESSAGE_TOO_LARGE` error. Since `retries` is set to `0`, the producer should immediately fail the futures for both messages in the batch with a `RecordTooLargeException`. The test should then complete successfully.

## Actual Result

The producer enters an infinite loop. The `flush()` call never completes, and the test hangs indefinitely. The logs show the `Sender` thread continuously retrying to send the same oversized batch.

This behavior is observed even when `retries` is configured to a value greater than 0 (e.g., 1, 2, or 10). The log message always indicates `(0 attempts left)`, which suggests the retry count is not being decremented for this specific error condition.

### Log Snippet

The following log message is repeated indefinitely:

```
[kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.producer.internals.Sender - [Producer clientId=producer-1] Got error produce response in correlation id [X] on topic-partition [Y], splitting and retrying (0 attempts left). Error: MESSAGE_TOO_LARGE
```

## Analysis

The issue seems to stem from the producer's handling of the `MESSAGE_TOO_LARGE` error. The `Sender` class logic attempts to split the batch and retry, which is a reasonable strategy for some scenarios. However, when a single message within the batch is larger than the broker's limit, splitting the batch is futile. 

Crucially, the retry mechanism for this specific error does not seem to respect the `retries` configuration. The producer should fail the entire batch immediately, especially when `retries` is set to `0`. For `retries` > 0, it should attempt to resend the batch according to the configuration and then fail. Instead, the current implementation leads to an endless loop regardless of the `retries` setting, consuming client resources and preventing the application from progressing or handling the error.
