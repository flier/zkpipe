package zkpipe

import java.util.Properties

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}

class LogBroker(uri: Uri,
                keySerializer: Serializer[Array[Byte]] = new ByteArraySerializer(),
                valueSerializer: Serializer[LogRecord]) extends LazyLogging {
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

    lazy val consumer = new KafkaConsumer(props)
    lazy val producer = new KafkaProducer(props, keySerializer, valueSerializer)
}
