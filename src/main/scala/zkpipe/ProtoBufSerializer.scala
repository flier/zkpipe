package zkpipe

import java.io.ByteArrayInputStream
import java.util

import ProtoBufConverters._
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Summary}
import org.apache.jute.BinaryInputArchive
import org.apache.kafka.common.serialization.Serializer
import org.apache.zookeeper.ZooDefs.OpCode
import org.apache.zookeeper.txn._
import zkpipe.TransactionOuterClass.{ACL, CheckVersion, Create, CreateContainer, CreateSession, Delete, Error, Header, Id, Message, SetACL, SetData, Transaction}

import scala.collection.JavaConverters._
import scala.language.postfixOps

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