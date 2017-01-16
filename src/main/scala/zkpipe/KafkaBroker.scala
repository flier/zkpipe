package zkpipe

import java.io.{Closeable, File}
import java.lang.Long
import java.util
import java.util.Properties

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Gauge, Histogram}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._

import scala.beans.{BeanInfoSkip, BeanProperty}
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.language.postfixOps

trait SendResult {
    val record: LogRecord
}

case class KafkaResult(record: LogRecord, metadata: RecordMetadata) extends SendResult

trait Broker extends Closeable {
    def send(log: LogRecord): Future[SendResult]
}

object KafkaBroker {
    val SUBSYSTEM: String = "kafka"
    val sending: Gauge = Gauge.build().subsystem(SUBSYSTEM).name("sending").labelNames("topic").help("Kafka sending messages").register()
    val sent: Counter = Counter.build().subsystem(SUBSYSTEM).name("sent").labelNames("topic").help("Kafka send succeeded messages").register()
    val error: Counter = Counter.build().subsystem(SUBSYSTEM).name("error").labelNames("topic").help("Kafka send failed message").register()
    val sendLatency: Histogram = Histogram.build().subsystem(SUBSYSTEM).name("send_latency").labelNames("topic").help("Kafka send latency").register()
}

trait KafkaBrokerMBean {
    @BeanInfoSkip
    val uri: Uri

    def getUri: String = uri.toString()

    def getTopic: String

    def close(): Unit
}

class KafkaBroker(val uri: Uri,
                  valueSerializer: Serializer[LogRecord])
    extends JMXExport with KafkaBrokerMBean with Broker with LazyLogging
{
    import KafkaBroker._

    mbean(this)

    val DEFAULT_KAFKA_HOST: String = "localhost"
    val DEFAULT_KAFKA_PORT: Int = 9092
    val DEFAULT_KAFKA_TOPIC: String = "zkpipe"

    require(uri.scheme.contains("kafka"), "Have to starts with kafka://")

    private lazy val props = {
        val props = new Properties()

        val host = uri.host.getOrElse(DEFAULT_KAFKA_HOST)
        val port = uri.port.getOrElse(DEFAULT_KAFKA_PORT)

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"$host:$port")

        uri.query.params.foreach { case (key, value) => props.put(key, value.get) }

        props
    }

    @BeanProperty
    lazy val topic: String = (uri.path split File.separator drop 1 headOption) getOrElse DEFAULT_KAFKA_TOPIC

    private var producerInitialized = false

    lazy val producer: KafkaProducer[Long, LogRecord] = {
        producerInitialized = true

        new KafkaProducer(props, new LongSerializer(), valueSerializer)
    }

    override def close(): Unit = {
        if (producerInitialized) {
            producer.flush()
            producer.close()
        }
    }

    override def send(log: LogRecord): Future[SendResult] = {
        val promise = Promise[SendResult]()

        sending.labels(topic).inc()

        val t = sendLatency.labels(topic).startTimer()

        producer.send(new ProducerRecord(topic, log.zxid, log),
            (metadata: RecordMetadata, exception: Exception) => {
                sending.labels(topic).dec()

                t.close()

                if (exception == null) {
                    sent.labels(topic).inc()

                    promise success KafkaResult(log, metadata)
                } else {
                    error.labels(topic).inc()

                    promise failure exception
                }
            }
        )

        promise.future
    }

    def latestZxid(): Option[Long] = {
        logger.debug(s"try to fetch latest zxid from Kafka topic `$topic`")

        val consumer = new KafkaConsumer[Long, Array[Byte]](props, new LongDeserializer(), new ByteArrayDeserializer())

        val partitions = consumer.partitionsFor(topic).asScala map {
            partitionInfo => new TopicPartition(topic, partitionInfo.partition())
        }

        val endOffsets = consumer.endOffsets(partitions asJava)

        logger.debug(s"found ${partitions.length} partitions: $endOffsets")

        val (partition, offset) = endOffsets.asScala.toSeq.sortBy(_._2).last

        consumer.assign(util.Arrays.asList(partition))
        consumer.seek(partition, offset - 1)

        val records = consumer.poll(1000).records(partition)

        if (records.isEmpty) None else Some(records.asScala.head.key())
    }
}
