package zkpipe

import java.io.ByteArrayInputStream
import java.util

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Histogram, Meter}
import org.apache.jute.BinaryInputArchive
import org.apache.kafka.common.serialization.Serializer
import org.apache.zookeeper.ZooDefs.OpCode
import org.apache.zookeeper.txn._
import zkpipe.JsonSerializer.metrics
import zkpipe.TransactionOuterClass.{ACL, CheckVersion, Create, CreateContainer, CreateSession, Delete, Error, Header, Id, Message, SetACL, SetData, Transaction}

import scala.collection.JavaConverters._
import scala.language.{implicitConversions, postfixOps}

object ProtoBufSerializer {
    val SUBSYSTEM: String = "pb"

    val encodeRecords: Meter = metrics.meter("encoded-records", SUBSYSTEM)
    val encodeBytes: Meter = metrics.meter("encoded-bytes", SUBSYSTEM)
    val recordSize: Histogram = metrics.histogram("record-size", SUBSYSTEM)

    implicit def toProtoBuf(txn: CreateTxn): Transaction =
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

    implicit def toProtoBuf(txn: CreateContainerTxn): Transaction =
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

    implicit def toProtoBuf(txn: DeleteTxn): Transaction =
        Transaction.newBuilder()
            .setDelete(
                Delete.newBuilder()
                    .setPath(txn.getPath)
            )
            .build()

    implicit def toProtoBuf(txn: SetDataTxn): Transaction =
        Transaction.newBuilder()
            .setSetData(
                SetData.newBuilder()
                    .setPath(txn.getPath)
                    .setData(ByteString.copyFrom(txn.getData))
                    .setVersion(txn.getVersion)
            )
            .build()

    implicit def toProtoBuf(txn: CheckVersionTxn): Transaction =
        Transaction.newBuilder()
            .setCheckVersion(
                CheckVersion.newBuilder()
                    .setPath(txn.getPath)
                    .setVersion(txn.getVersion)
            )
            .build()


    implicit def toProtoBuf(txn: SetACLTxn): Transaction =
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

    implicit def toProtoBuf(txn: CreateSessionTxn): Transaction =
        Transaction.newBuilder()
            .setCreateSession(
                CreateSession.newBuilder()
                    .setTimeOut(txn.getTimeOut)
            )
            .build()

    implicit def toProtoBuf(txn: ErrorTxn): Transaction =
        Transaction.newBuilder()
            .setError(
                Error.newBuilder()
                    .setErrno(txn.getErr)
            )
            .build()

    implicit def toProtoBuf(txn: MultiTxn): Transaction = {
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

class ProtoBufSerializer extends Serializer[LogRecord] with LazyLogging {
    import ProtoBufSerializer._

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, log: LogRecord): Array[Byte] = {
        val record: Option[Transaction] = log.record match {
            case r: CreateTxn  => Some(r)
            case r: CreateContainerTxn  => Some(r)
            case r: DeleteTxn  => Some(r)
            case r: SetDataTxn  => Some(r)
            case r: CheckVersionTxn  => Some(r)
            case r: SetACLTxn  => Some(r)
            case r: CreateSessionTxn => Some(r)
            case r: ErrorTxn => Some(r)
            case r: MultiTxn => Some(r)
            case _ => None
        }

        val builder = Message.newBuilder().setHeader(
            Header.newBuilder()
                .setSession(log.session)
                .setCxid(log.cxid)
                .setZxid(log.zxid)
                .setTime(log.time.getMillis)
                .setPath(log.path.getOrElse(""))
                .setType(Transaction.Type.forNumber(log.opcode.id))
        )

        record foreach { builder.setRecord }

        val bytes = builder.build().toByteArray

        encodeRecords.mark()
        encodeBytes.mark(bytes.length)
        recordSize += bytes.length

        bytes
    }

    override def close(): Unit = {}
}