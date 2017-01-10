package zkpipe

import java.util.Properties

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Gauge, Histogram}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, Serializer}

import scala.concurrent.{Future, Promise}

trait LogMetadata {}

case class KafkaMetadata(metadata: RecordMetadata) extends LogMetadata

trait Broker {
    def send(log: LogRecord): Future[LogMetadata]
}

object LogBrokerMetrics {
    val SUBSYSTEM: String = "kafka"
    val sending: Gauge = Gauge.build().subsystem(SUBSYSTEM).name("sending").labelNames("topic").help("Kafka sending messages").register()
    val sent: Counter = Counter.build().subsystem(SUBSYSTEM).name("sent").labelNames("topic").help("Kafka send succeeded messages").register()
    val error: Counter = Counter.build().subsystem(SUBSYSTEM).name("error").labelNames("topic").help("Kafka send failed message").register()
    val sendLatency: Histogram = Histogram.build().subsystem(SUBSYSTEM).name("send_latency").labelNames("topic").help("Kafka send latency").register()
}

class LogBroker(uri: Uri,
                valueSerializer: Serializer[LogRecord]) extends Broker with LazyLogging {
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

    lazy val producer = new KafkaProducer(props, new LongSerializer(), valueSerializer)

    override def send(log: LogRecord): Future[LogMetadata] = {
        import LogBrokerMetrics._

        val promise = Promise[LogMetadata]()

        sending.labels(topic).inc()

        val t = sendLatency.labels(topic).startTimer()

        producer.send(new ProducerRecord(topic, log.zxid, log),
            (metadata: RecordMetadata, exception: Exception) => {
                sending.labels(topic).dec()

                t.close()

                if (exception == null) {
                    sent.labels(topic).inc()

                    promise success KafkaMetadata(metadata)
                } else {
                    error.labels(topic).inc()

                    promise failure exception
                }
            }
        )

        promise.future
    }
}
