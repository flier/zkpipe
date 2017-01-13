package zkpipe

import java.io.Closeable
import java.lang.Long
import java.util.Properties

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Gauge, Histogram}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, Serializer}

import scala.concurrent.{Future, Promise}

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

class KafkaBroker(uri: Uri, valueSerializer: Serializer[LogRecord]) extends Broker with LazyLogging
{
    import KafkaBroker._

    require(uri.scheme.contains("kafka"), "Have to starts with kafka://")

    private lazy val props = {
        val props = new Properties()

        for (host <- uri.host; port <- uri.port) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"$host:$port")
        }

        uri.query.params.foreach { case (key, value) => props.put(key, value.get) }

        props
    }

    lazy val topic: String = uri.path

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
}
