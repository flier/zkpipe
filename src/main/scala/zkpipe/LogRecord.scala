package zkpipe

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import org.apache.zookeeper.ZooDefs.OpCode
import com.github.nscala_time.time.Imports._
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Summary}
import org.apache.jute.{BinaryInputArchive, Record}
import org.apache.kafka.common.serialization.Serializer
import org.apache.zookeeper.server.util.SerializeUtils
import org.apache.zookeeper.txn._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import zkpipe.TransactionOuterClass._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Try

object LogRecordMetrics {
    val SUBSYSTEM: String = "decode"
    val decodeRecords: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").labelNames("type").help("decoded records").register()
    val decodeBytes: Counter = Counter.build().subsystem(SUBSYSTEM).name("bytes").help("decoded bytes").register()
}

class LogRecord(val bytes: Array[Byte]) extends LazyLogging {
    object TxnType extends Enumeration {
        type Type = Value

        val notification = Value(OpCode.notification)
        val create = Value(OpCode.create)
        val delete = Value(OpCode.delete)
        val exists = Value(OpCode.exists)
        val getData = Value(OpCode.getData)
        val setData = Value(OpCode.setData)
        val getACL = Value(OpCode.getACL)
        val setACL = Value(OpCode.setACL)
        val getChildren = Value(OpCode.getChildren)
        val sync = Value(OpCode.sync)
        val ping = Value(OpCode.ping)
        val getChildren2 = Value(OpCode.getChildren2)
        val check = Value(OpCode.check)
        val multi = Value(OpCode.multi)
        val create2 = Value(OpCode.create2)
        val reconfig = Value(OpCode.reconfig)
        val checkWatches = Value(OpCode.checkWatches)
        val removeWatches = Value(OpCode.removeWatches)
        val createContainer = Value(OpCode.createContainer)
        val deleteContainer = Value(OpCode.deleteContainer)
        val auth = Value(OpCode.auth)
        val setWatches = Value(OpCode.setWatches)
        val sasl = Value(OpCode.sasl)
        val createSession = Value(OpCode.createSession)
        val closeSession = Value(OpCode.closeSession)
        val error = Value(OpCode.error)
    }

    import TxnType._

    import LogRecordMetrics._

    val header: TxnHeader = new TxnHeader()
    val record: Record = SerializeUtils.deserializeTxn(bytes, header)

    lazy val session: Long = header.getClientId
    lazy val cxid: Int = header.getCxid
    lazy val zxid: Long = header.getZxid
    lazy val time: DateTime = header.getTime.toDateTime
    lazy val opcode: Type = apply(header.getType)
    lazy val path: Option[String] = record match {
        case r: CreateTxn  => Some(r.getPath)
        case r: CreateContainerTxn  => Some(r.getPath)
        case r: DeleteTxn  => Some(r.getPath)
        case r: SetDataTxn  => Some(r.getPath)
        case r: CheckVersionTxn  => Some(r.getPath)
        case r: SetACLTxn  => Some(r.getPath)
        case _ => None
    }

    decodeRecords.labels(opcode.toString).inc()
    decodeBytes.inc(bytes.length)
}

import ProtoBufConverters._

object ProtoBufConverters {
    implicit def toProtoBuf(implicit txn: CreateTxn): Transaction =
        Transaction.newBuilder()
            .setCreate(
                Create.newBuilder()
                    .setPath(txn.getPath)
                    .setData(ByteString.copyFrom(txn.getData))
                    .addAllAcl(
                        txn.getAcl.asScala map { acl =>
                            ACL.newBuilder()
                                .setPerms(acl.getPerms)
                                .setId(Id.newBuilder()
                                    .setScheme(acl.getId.getScheme)
                                    .setId(acl.getId.getId)
                                ).build()
                        } asJava)
                    .setEphemeral(txn.getEphemeral)
                    .setParentCVersion(txn.getParentCVersion)
            )
            .build()

    implicit def toProtoBuf(implicit txn: CreateContainerTxn): Transaction =
        Transaction.newBuilder()
            .setCreateContainer(
                CreateContainer.newBuilder()
                    .setPath(txn.getPath)
                    .setData(ByteString.copyFrom(txn.getData))
                    .addAllAcl(
                        txn.getAcl.asScala map { acl =>
                            ACL.newBuilder()
                                .setPerms(acl.getPerms)
                                .setId(Id.newBuilder()
                                    .setScheme(acl.getId.getScheme)
                                    .setId(acl.getId.getId)
                                ).build()
                        } asJava)
                    .setParentCVersion(txn.getParentCVersion)
            )
            .build()

    implicit def toProtoBuf(implicit txn: DeleteTxn): Transaction =
        Transaction.newBuilder()
            .setDelete(
                Delete.newBuilder()
                    .setPath(txn.getPath)
            )
            .build()

    implicit def toProtoBuf(implicit txn: SetDataTxn): Transaction =
        Transaction.newBuilder()
            .setSetData(
                SetData.newBuilder()
                    .setPath(txn.getPath)
                    .setData(ByteString.copyFrom(txn.getData))
                    .setVersion(txn.getVersion)
            )
            .build()

    implicit def toProtoBuf(implicit txn: CheckVersionTxn): Transaction =
        Transaction.newBuilder()
            .setCheckVersion(
                CheckVersion.newBuilder()
                    .setPath(txn.getPath)
                    .setVersion(txn.getVersion)
            )
            .build()


    implicit def toProtoBuf(implicit txn: SetACLTxn): Transaction =
        Transaction.newBuilder()
            .setSetACL(
                SetACL.newBuilder()
                    .setPath(txn.getPath)
                    .addAllAcl(
                        txn.getAcl.asScala map { acl =>
                            ACL.newBuilder()
                                .setPerms(acl.getPerms)
                                .setId(Id.newBuilder()
                                    .setScheme(acl.getId.getScheme)
                                    .setId(acl.getId.getId)
                                ).build()
                        } asJava)
                    .setVersion(txn.getVersion)
            )
            .build()

    implicit def toProtoBuf(implicit txn: CreateSessionTxn): Transaction =
        Transaction.newBuilder()
            .setCreateSession(
                CreateSession.newBuilder()
                    .setTimeOut(txn.getTimeOut)
            )
            .build()

    implicit def toProtoBuf(implicit txn: ErrorTxn): Transaction =
        Transaction.newBuilder()
            .setError(
                Error.newBuilder()
                    .setErrno(txn.getErr)
            )
            .build()

    implicit def toProtoBuf(implicit txn: MultiTxn): Transaction = {
        val builder = Transaction.newBuilder()

        txn.getTxns.asScala foreach { txn =>
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

            record match {
                case r: CreateTxn => toProtoBuf(r)
                case r: CreateContainerTxn => toProtoBuf(r)
                case r: DeleteTxn => toProtoBuf(r)
                case r: SetDataTxn => toProtoBuf(r)
                case r: CheckVersionTxn => toProtoBuf(r)
            }
        }

        builder.build()
    }
}

import JsonConverters._

object JsonConverters {
    implicit def toJson(implicit txn: CreateTxn): JValue =
        "create" ->
            ("path" -> txn.getPath) ~
            ("data" -> BaseEncoding.base64().encode(txn.getData)) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("ephemeral" -> txn.getEphemeral) ~
            ("parentCVersion" -> txn.getParentCVersion)

    implicit def toJson(implicit txn: CreateContainerTxn): JValue =
        "create-container" ->
            ("path" -> txn.getPath) ~
            ("data" -> BaseEncoding.base64().encode(txn.getData)) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("parentCVersion" -> txn.getParentCVersion)

    implicit def toJson(implicit txn: DeleteTxn): JValue =
        "delete" -> ("path" -> txn.getPath)

    implicit def toJson(implicit txn: SetDataTxn): JValue =
        "set-data" ->
            ("path" -> txn.getPath) ~
            ("data" -> BaseEncoding.base64().encode(txn.getData)) ~
            ("version" -> txn.getVersion)

    implicit def toJson(implicit txn: CheckVersionTxn): JValue =
        "check-version" ->
            ("path" -> txn.getPath) ~
            ("version" -> txn.getVersion)

    implicit def toJson(implicit txn: SetACLTxn): JValue =
        "set-acl" ->
            ("path" -> txn.getPath) ~
            ("acl" -> txn.getAcl.asScala.map({ acl =>
                ("scheme" -> acl.getId.getScheme) ~ ("id" -> acl.getId.getId) ~ ("perms" -> acl.getPerms)
            })) ~
            ("version" -> txn.getVersion)

    implicit def toJson(implicit txn: CreateSessionTxn): JValue =
        "create-session" -> ("timeout" -> txn.getTimeOut)

    implicit def toJson(implicit txn: ErrorTxn): JValue =
        "error" -> ("errno" -> txn.getErr)

    implicit def toJson(implicit txn: MultiTxn): JValue = {
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

            record match {
                case r: CreateTxn => toJson(r)
                case r: CreateContainerTxn => toJson(r)
                case r: DeleteTxn => toJson(r)
                case r: SetDataTxn => toJson(r)
                case r: CheckVersionTxn => toJson(r)
            }
        } toList

        "multi" -> records
    }
}

object ProtoBufSerializerMetrics {
    val SUBSYSTEM: String = "pb"
    val records: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").labelNames("type").help("encoded protobuf messages").register()
    val size: Summary = Summary.build().subsystem(SUBSYSTEM).name("size").help("size of encoded protobuf messages").register()
}

class ProtoBufSerializer extends Serializer[LogRecord] with LazyLogging {
    import ProtoBufSerializerMetrics._

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, log: LogRecord): Array[Byte] = {
        val txn: Transaction = log.record match {
            case r: CreateTxn  => records.labels("create").inc(); toProtoBuf(r)
            case r: CreateContainerTxn  => records.labels("createContainer").inc(); toProtoBuf(r)
            case r: DeleteTxn  => records.labels("delete").inc(); toProtoBuf(r)
            case r: SetDataTxn  => records.labels("setData").inc(); toProtoBuf(r)
            case r: CheckVersionTxn  => records.labels("checkVersion").inc(); toProtoBuf(r)
            case r: SetACLTxn  => records.labels("setACL").inc(); toProtoBuf(r)
            case r: CreateSessionTxn => records.labels("createSession").inc(); toProtoBuf(r)
            case r: ErrorTxn => records.labels("error").inc(); toProtoBuf(r)
            case r: MultiTxn => records.labels("multi").inc(); toProtoBuf(r)
        }

        val msg: Message = Message.newBuilder().setHeader(
            Header.newBuilder()
                .setSession(log.session)
                .setCxid(log.cxid)
                .setZxid(log.zxid)
                .setTime(log.time.getMillis)
                .setPath(log.path.getOrElse(""))
                .setType(Transaction.Type.forNumber(log.opcode.id))
        ).setRecord(txn).build()

        val bytes = msg.toByteArray

        size.observe(bytes.length)

        bytes
    }

    override def close(): Unit = {}
}

object JsonSerializerMetrics {
    val SUBSYSTEM: String = "json"
    val records: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").labelNames("type").help("encoded JSON messages").register()
    val size: Summary = Summary.build().subsystem(SUBSYSTEM).name("size").help("size of encoded JSON messages").register()
}

class JsonSerializer(var props: mutable.Map[String, Any] = mutable.Map[String, Any]())
    extends Serializer[LogRecord] with LazyLogging
{
    import JsonSerializerMetrics._

    val PROPERTY_PRETTY = "pretty"

    lazy val printPretty: Boolean = Try(props.get(PROPERTY_PRETTY).toString.toBoolean).getOrElse(false)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        props ++= configs.asScala.toMap
    }

    override def serialize(topic: String, log: LogRecord): Array[Byte] = {
        val record: JValue = log.record match {
            case r: CreateTxn => records.labels("create").inc(); toJson(r)
            case r: CreateContainerTxn => records.labels("createContainer").inc(); toJson(r)
            case r: DeleteTxn => records.labels("delete").inc(); toJson(r)
            case r: SetDataTxn => records.labels("setData").inc(); toJson(r)
            case r: CheckVersionTxn => records.labels("checkVersion").inc(); toJson(r)
            case r: SetACLTxn => records.labels("setACL").inc(); toJson(r)
            case r: CreateSessionTxn => records.labels("createSession").inc(); toJson(r)
            case r: ErrorTxn => records.labels("error").inc(); toJson(r)
            case r: MultiTxn => records.labels("multi").inc(); toJson(r)
        }

        val json: JValue = render(
            ("session" -> log.session) ~
            ("cxid" -> log.cxid) ~
            ("zxid" -> log.zxid) ~
            ("time" -> log.time.getMillis) ~
            ("path" -> log.path) ~
            ("type" -> log.opcode.toString) ~
            ("record" -> record)
        )

        val bytes = (if (printPretty) { pretty(json) } else { compact(json) }).getBytes(UTF_8)

        size.observe(bytes.length)

        bytes
    }

    override def close(): Unit = {}
}

object RawSerializerMetrics {
    val SUBSYSTEM = "raw"
    val records: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").help("encoded raw messages").register()
    val size: Summary = Summary.build().subsystem(SUBSYSTEM).name("size").help("size of encoded JSON messages").register()
}

class RawSerializer extends Serializer[LogRecord] with LazyLogging {
    import RawSerializerMetrics._

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: LogRecord): Array[Byte] = {
        records.inc()

        data.bytes
    }

    override def close(): Unit = {}
}