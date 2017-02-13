package zkpipe

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.zookeeper.ZooDefs.{OpCode, Perms}
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.Counter
import nl.grons.metrics.scala.{DefaultInstrumented, Histogram, Meter}
import org.apache.jute.{BinaryOutputArchive, OutputArchive, Record}
import org.apache.zookeeper.data.{ACL, StatPersisted}
import org.apache.zookeeper.server.util.SerializeUtils
import org.apache.zookeeper.txn._

import scala.language.postfixOps
import java.util

import zkpipe.TxnTypes.TxnType

object LogRecord extends DefaultInstrumented {
    val SUBSYSTEM: String = "decode"

    val recordByType: Counter = Counter.build().subsystem(SUBSYSTEM).name("records").labelNames("type").help("decoded records").register()

    val totalRecords: Meter = metrics.meter("total-records")
    val totalBytes: Meter = metrics.meter("total-bytes")
    val recordSize: Histogram = metrics.histogram("record-size")
}

object TxnTypes extends Enumeration {
    type TxnType = Value

    val Notification = Value(OpCode.notification)
    val Create = Value(OpCode.create)
    val Delete = Value(OpCode.delete)
    val Exists = Value(OpCode.exists)
    val GetData = Value(OpCode.getData)
    val SetData = Value(OpCode.setData)
    val GetACL = Value(OpCode.getACL)
    val SetACL = Value(OpCode.setACL)
    val GetChildren = Value(OpCode.getChildren)
    val Sync = Value(OpCode.sync)
    val Ping = Value(OpCode.ping)
    val GetChildren2 = Value(OpCode.getChildren2)
    val Check = Value(OpCode.check)
    val Multi = Value(OpCode.multi)
    val Create2 = Value(OpCode.create2)
    val Reconfig = Value(OpCode.reconfig)
    val CheckWatches = Value(OpCode.checkWatches)
    val RemoveWatches = Value(OpCode.removeWatches)
    val CreateContainer = Value(OpCode.createContainer)
    val DeleteContainer = Value(OpCode.deleteContainer)
    val Auth = Value(OpCode.auth)
    val SetWatches = Value(OpCode.setWatches)
    val Sasl = Value(OpCode.sasl)
    val CreateSession = Value(OpCode.createSession)
    val CloseSession = Value(OpCode.closeSession)
    val Error = Value(OpCode.error)
}

object AclPerms extends Enumeration {
    type AclPerm = Value

    val Read = Value(Perms.READ)
    val Write = Value(Perms.WRITE)
    val Create = Value(Perms.CREATE)
    val Delete = Value(Perms.DELETE)
    val Admin = Value(Perms.ADMIN)
    val All = Value(Perms.ALL)
}

trait LogRecord {
    val bytes: Array[Byte]
    val record: Record
    val session: Long
    val cxid: Int
    val zxid: Long
    val time: DateTime
    val opcode: TxnType
    val path: Option[String]

    def serialize(out: OutputArchive, tag: String): Unit
}

class TransactionLog(val bytes: Array[Byte]) extends LogRecord with LazyLogging  {
    import LogRecord._
    import TxnTypes._

    val header: TxnHeader = new TxnHeader()
    val record: Record = SerializeUtils.deserializeTxn(bytes, header)

    lazy val session: Long = header.getClientId
    lazy val cxid: Int = header.getCxid
    lazy val zxid: Long = header.getZxid
    lazy val time: DateTime = header.getTime.toDateTime
    lazy val opcode: TxnType = apply(header.getType)
    lazy val path: Option[String] = record match {
        case r: CreateTxn  => Some(r.getPath)
        case r: CreateContainerTxn  => Some(r.getPath)
        case r: DeleteTxn  => Some(r.getPath)
        case r: SetDataTxn  => Some(r.getPath)
        case r: CheckVersionTxn  => Some(r.getPath)
        case r: SetACLTxn  => Some(r.getPath)
        case _ => None
    }

    recordByType.labels(opcode.toString).inc()
    totalRecords.mark()
    totalBytes.mark(bytes.length)
    recordSize += bytes.length

    def serialize(out: OutputArchive, tag: String): Unit = {
        out.writeBuffer(bytes, "record")
    }
}

class DataNode(val _path: String,
               val data: Array[Byte],
               val acl: util.List[ACL],
               val stat: StatPersisted) extends LogRecord with LazyLogging {
    lazy val record: Record = new CreateTxn(_path, data, acl, stat.getEphemeralOwner != 0, stat.getPzxid.toInt)
    lazy val session: Long = 0
    lazy val cxid: Int = stat.getCzxid.toInt
    lazy val zxid: Long = stat.getMzxid
    lazy val time: DateTime = stat.getMtime.toDateTime
    lazy val opcode: TxnType = TxnTypes.Create
    lazy val path: Option[String] = Some(_path)

    def serialize(out: OutputArchive, tag: String): Unit = {
        val hdr = new TxnHeader(session, cxid, zxid, stat.getMtime, opcode.id)
        hdr.serialize(out, "hdr")
        record.serialize(out, "txn")
    }

    lazy val bytes: Array[Byte] = {
        val stream = new ByteArrayOutputStream()
        val out = new BinaryOutputArchive(new DataOutputStream(stream))
        record.serialize(out, "record")
        stream.toByteArray
    }
}