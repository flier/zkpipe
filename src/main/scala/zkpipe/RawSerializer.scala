package zkpipe

import java.util

import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Summary}
import org.apache.kafka.common.serialization.Serializer

object RawSerializer {
    val SUBSYSTEM = "raw"
    val records: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").help("encoded raw messages").register()
    val size: Summary = Summary.build().subsystem(SUBSYSTEM).name("size").help("size of encoded JSON messages").register()
}

class RawSerializer extends Serializer[LogRecord] with LazyLogging {
    import RawSerializer._

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: LogRecord): Array[Byte] = {
        records.inc()
        size.observe(data.bytes.length)

        data.bytes
    }

    override def close(): Unit = {}
}