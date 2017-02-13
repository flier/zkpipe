package zkpipe

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import com.google.common.io.BaseEncoding
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{DefaultInstrumented, Histogram, Meter}
import org.apache.jute.BinaryInputArchive
import org.apache.kafka.common.serialization.Serializer
import org.apache.zookeeper.ZooDefs.OpCode
import org.apache.zookeeper.txn._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try

object JsonSerializer extends DefaultInstrumented {
    val SUBSYSTEM: String = "json"

    val encodeRecords: Meter = metrics.meter("encoded-records", SUBSYSTEM)
    val encodeBytes: Meter = metrics.meter("encoded-bytes", SUBSYSTEM)
    val recordSize: Histogram = metrics.histogram("record-size", SUBSYSTEM)

    val PRETTY_PRINT = "pretty-print"

    def encodeData(bytes: Array[Byte]): JValue =
        if (bytes == null)
            JNull
        else {
            val s = new String(bytes, UTF_8);

            if (s.chars().anyMatch(c => Character.isISOControl(c))) {
                "base64" -> BaseEncoding.base64().encode(bytes)
            } else try {
                parse(StringInput(s))
            } catch {
                case _: Throwable => JString(s)
            }
        }

    implicit def toJson(txn: CreateTxn): JValue =
        "create" ->
            ("path" -> txn.getPath) ~
            ("data" -> encodeData(txn.getData)) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("ephemeral" -> txn.getEphemeral) ~
            ("parentCVersion" -> txn.getParentCVersion)

    implicit def toJson(txn: CreateContainerTxn): JValue =
        "create-container" ->
            ("path" -> txn.getPath) ~
            ("data" -> encodeData(txn.getData)) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("parentCVersion" -> txn.getParentCVersion)

    implicit def toJson(txn: DeleteTxn): JValue =
        "delete" -> ("path" -> txn.getPath)

    implicit def toJson(txn: SetDataTxn): JValue =
        "set-data" ->
            ("path" -> txn.getPath) ~
            ("data" -> encodeData(txn.getData)) ~
            ("version" -> txn.getVersion)

    implicit def toJson(txn: CheckVersionTxn): JValue =
        "check-version" ->
            ("path" -> txn.getPath) ~
            ("version" -> txn.getVersion)

    implicit def toJson(txn: SetACLTxn): JValue =
        "set-acl" ->
            ("path" -> txn.getPath) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("version" -> txn.getVersion)

    implicit def toJson(txn: CreateSessionTxn): JValue =
        "create-session" -> ("timeout" -> txn.getTimeOut)

    implicit def toJson(txn: ErrorTxn): JValue =
        "error" -> ("errno" -> txn.getErr)

    implicit def toJson(txn: MultiTxn): JValue = {
        val records = txn.getTxns.asScala map { txn =>
            val record = txn.getType match {
                case OpCode.create => new CreateTxn
                case OpCode.createContainer => new CreateContainerTxn
                case OpCode.delete => new DeleteTxn
                case OpCode.setData => new SetDataTxn
                case OpCode.check => new CheckVersionTxn
            }

            record.deserialize(
                BinaryInputArchive.getArchive(
                    new ByteArrayInputStream(txn.getData)), "txn")

            val v: JValue = record match {
                case r: CreateTxn => r
                case r: CreateContainerTxn => r
                case r: DeleteTxn => r
                case r: SetDataTxn => r
                case r: CheckVersionTxn => r
            }

            v
        }

        "multi" -> records
    }
}

class JsonSerializer(prettyPrint: Boolean = false) extends Serializer[LogRecord] with LazyLogging
{
    import JsonSerializer._

    var props: mutable.Map[String, Any] = mutable.Map()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        props ++= configs.asScala.toMap
    }

    override def serialize(topic: String, log: LogRecord): Array[Byte] = {
        val record: Option[JValue] = log.record match {
            case r: CreateTxn => Some(r)
            case r: CreateContainerTxn => Some(r)
            case r: DeleteTxn => Some(r)
            case r: SetDataTxn => Some(r)
            case r: CheckVersionTxn => Some(r)
            case r: SetACLTxn => Some(r)
            case r: CreateSessionTxn => Some(r)
            case r: ErrorTxn => Some(r)
            case r: MultiTxn => Some(r)
            case _ => None
        }

        val json: JValue = render(
            ("session" -> log.session) ~
            ("cxid" -> log.cxid) ~
            ("zxid" -> log.zxid) ~
            ("time" -> log.time.getMillis) ~
            ("path" -> log.path) ~
            ("type" -> log.opcode.toString) ~
            ("record" -> record.orNull)
        )

        val bytes = (if (Try(props.get(PRETTY_PRINT).toString.toBoolean).getOrElse(prettyPrint)) {
            pretty(json)
        } else {
            compact(json)
        }).getBytes(UTF_8)

        encodeRecords.mark()
        encodeBytes.mark(bytes.length)
        recordSize += bytes.length

        bytes
    }

    override def close(): Unit = {}
}
