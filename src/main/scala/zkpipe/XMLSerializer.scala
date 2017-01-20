package zkpipe

import java.io.ByteArrayInputStream
import java.util
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.BaseEncoding
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{DefaultInstrumented, Histogram, Meter}
import org.apache.jute.BinaryInputArchive
import org.apache.kafka.common.serialization.Serializer
import org.apache.zookeeper.ZooDefs.OpCode
import org.apache.zookeeper.txn._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try
import scala.xml._

object XMLSerializer extends DefaultInstrumented {
    val SUBSYSTEM: String = "xml"

    val encodeRecords: Meter = metrics.meter("encoded-records", SUBSYSTEM)
    val encodeBytes: Meter = metrics.meter("encoded-bytes", SUBSYSTEM)
    val recordSize: Histogram = metrics.histogram("record-size", SUBSYSTEM)

    val PRETTY_PRINT = "pretty-print"

    def base64(bytes: Array[Byte]): String = if (bytes == null) "" else BaseEncoding.base64().encode(bytes)

    implicit def toXML(txn: CreateTxn): Elem =
        <create path={txn.getPath}
                ephemeral={txn.getEphemeral.toString}
                parentCVersion={txn.getParentCVersion.toString}>
            <data>{ PCData(base64(txn.getData)) }</data>
        { txn.getAcl.asScala.map({ acl =>
            <acl scheme={acl.getId.getScheme}
                 id={acl.getId.getId}
                 perms={acl.getPerms.toString}/>
        }) }
        </create>

    implicit def toXML(txn: CreateContainerTxn): Elem =
        <create-container path={txn.getPath}
                          parentCVersion={txn.getParentCVersion.toString}>
            <data>{ PCData(base64(txn.getData)) }</data>

            { txn.getAcl.asScala.map({ acl =>
                <acl scheme={acl.getId.getScheme}
                     id={acl.getId.getId}
                     perms={acl.getPerms.toString}/>
        }) }
        </create-container>

    implicit def toXML(txn: DeleteTxn): Elem =
        <delete path={txn.getPath}/>

    implicit def toXML(txn: SetDataTxn): Elem =
        <set-data path={txn.getPath}
                  version={txn.getVersion.toString}>
            <data>{ PCData(base64(txn.getData)) }</data>
        </set-data>

    implicit def toXML(txn: CheckVersionTxn): Elem =
        <check-version path={txn.getPath}
                       version={txn.getVersion.toString}/>

    implicit def toXML(txn: SetACLTxn): Elem =
        <set-acl path={txn.getPath}
                 version={txn.getVersion.toString}>
        { txn.getAcl.asScala.map({ acl =>
            <acl scheme={acl.getId.getScheme}
                 id={acl.getId.getId}
                 perms={acl.getPerms.toString}/>
        }) }
        </set-acl>

    implicit def toXML(txn: CreateSessionTxn): Elem =
        <delete timeout={txn.getTimeOut.toString}/>

    implicit def toXML(txn: ErrorTxn): Elem =
        <error errno={txn.getErr.toString}/>

    implicit def toXML(txn: MultiTxn): Elem = {
        <multi>
        { txn.getTxns.asScala map { txn =>
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
                case r: CreateTxn => r
                case r: CreateContainerTxn => r
                case r: DeleteTxn => r
                case r: SetDataTxn => r
                case r: CheckVersionTxn => r
            }
        } }
        </multi>
    }
}

class XMLSerializer(prettyPrint: Boolean = false) extends Serializer[LogRecord] with LazyLogging
{
    import XMLSerializer._

    var props: mutable.Map[String, Any] = mutable.Map()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        props ++= configs.asScala.toMap
    }

    override def serialize(topic: String, log: LogRecord): Array[Byte] = {
        val record: Option[Elem] = log.record match {
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

        val node = <record session={log.session.toString}
                           cxid={log.cxid.toString}
                           zxid={log.zxid.toString}
                           time={log.time.getMillis.toString}
                           path={log.path.orNull}
                           type={log.opcode.toString}>
            {record.orNull}
        </record>

        val xml = if (Try(props.get(PRETTY_PRINT).toString.toBoolean).getOrElse(prettyPrint)) {
            new PrettyPrinter(80, 2).format(node)
        } else {
            scala.xml.Utility.trim(node).toString()
        }

        val bytes = xml.getBytes(UTF_8)

        encodeRecords.mark()
        encodeBytes.mark(bytes.length)
        recordSize += bytes.length

        bytes
    }

    override def close(): Unit = {}
}
