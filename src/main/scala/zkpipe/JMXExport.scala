package zkpipe

import java.lang.management.ManagementFactory
import java.util
import javax.management.{MBeanServer, ObjectInstance, ObjectName}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class JMXBean(instance: ObjectInstance) extends LazyLogging {
    import JMXExport._

    require(instance != null, "instance must not be null")

    def unregister(): Unit = mBeanServer.unregisterMBean(instance.getObjectName)
}

object JMXExport extends LazyLogging {
    logger.info(s"register MBeans to ${ManagementFactory.getRuntimeMXBean.getName}")

    val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer
}

trait JMXExport {
    import JMXExport._

    implicit def stringToObjectName(name: String): ObjectName = new ObjectName(name)

    def registerMBean(obj: Object, name: ObjectName = null): JMXBean = {
        val clazz = obj.getClass

        new JMXBean(mBeanServer.registerMBean(obj, if (name != null) name else {
            new ObjectName(clazz.getPackage.getName, "type", clazz.getSimpleName)
        }))
    }

    def registerMBean(obj: Object, props: Map[String, String]): JMXBean = {
        val clazz = obj.getClass

        new JMXBean(mBeanServer.registerMBean(obj,
            new ObjectName(clazz.getPackage.getName,
                new util.Hashtable((Map("type" -> clazz.getSimpleName) ++ props).asJava))
        ))
    }

    def mbean(obj: Object, name: ObjectName = null): JMXBean = { registerMBean(obj, name) }
    def mbean(obj: Object, props: Map[String, String]): JMXBean = { registerMBean(obj, props) }
}