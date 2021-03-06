package zkpipe

import java.io.{Closeable, File}
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}
import java.nio.file.{FileSystems, Path, WatchKey, WatchService}

import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.Counter
import nl.grons.metrics.scala.{DefaultInstrumented, Meter}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object LogWatcher extends DefaultInstrumented {
    val SUBSYSTEM = "watcher"

    val fileChangesByteType: Counter = Counter.build().subsystem(SUBSYSTEM).name("changes").labelNames("dir", "kind").help("watched file changes").register()

    val fileChanges: Meter = metrics.meter("file-changes", SUBSYSTEM)
}

class LogWatcher(val dir: File,
                 val checkCrc: Boolean = true,
                 val fromLatest: Boolean = false)
    extends JMXExport with LogWatcherMBean with Closeable with LazyLogging
{
    import LogWatcher._

    mbean(this)

    require(dir.isDirectory, "can only watch directory")

    var watchFiles: mutable.Map[Path, LogFile] = mutable.Map.empty ++
        (dir.listFiles filter { _.isFile } filter { _.canRead } flatMap { file =>
            val filename = file.getAbsoluteFile.toPath

            Try (new LogFile(file)) match {
                case Success(logFile) =>
                    if (logFile.isLog) {
                        Some((logFile.filepath, logFile))
                    } else {
                        logger.info(s"skip invalid file $filename")

                        logFile.close()

                        None
                    }
                case Failure(err) =>
                    logger.info(s"skip invalid file $filename, err=$err")

                    None
            }
        })

    if (fromLatest) {
        val (latest, skipped) = watchFiles.values.toSeq.sortBy(_.firstZxid).reverse.splitAt(1)

        latest foreach { logFile =>
            logger.debug(s"log file `${logFile.filename}` skipped to end")

            logFile.skipToEnd
        }

        skipped foreach { logFile =>
            logger.debug(s"log file `${logFile.filename}` skipped and closed")

            watchFiles.remove(logFile.filepath).foreach(_.close())
        }
    }

    private val watcher: WatchService = FileSystems.getDefault.newWatchService()

    val watchKeys: mutable.Map[WatchKey, Path] = mutable.Map.empty ++
        Map(dir.toPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY) -> dir.toPath)

    logger.info(s"watching directory $dir ...")

    override def close(): Unit = {
        watcher.close()
        watchFiles.values foreach { _.close() }
    }

    val changedFiles: Stream[LogFile] = {
        var iter = if (fromLatest) {
            poll().iterator
        } else {
            watchFiles.values.toSeq.sortBy(_.filename).iterator
        }

        def next(): LogFile = {
            while (!iter.hasNext) {
                iter = poll().iterator
            }

            iter.next()
        }

        Stream.continually(next)
    }

    def poll(): Iterable[LogFile] = {
        val watchKey = watcher.take()

        logger.debug("found changes", watchKey.isValid)

        val changedFiles = watchKey.pollEvents().asScala flatMap { watchEvent =>
            val dirname = watchKeys(watchKey)
            val filename = dirname.resolve(watchEvent.context.asInstanceOf[Path])

            logger.info(s"file `$filename` {}", watchEvent.kind match {
                case ENTRY_CREATE => "created"
                case ENTRY_DELETE => "deleted"
                case ENTRY_MODIFY => "modified"
            })

            fileChanges.mark()

            watchEvent.kind match {
                case ENTRY_CREATE =>
                    fileChangesByteType.labels(dirname.toString, "create").inc()

                    Some(watchFiles.getOrElseUpdate(filename, new LogFile(filename.toFile, checkCrc = checkCrc)))

                case ENTRY_DELETE =>
                    fileChangesByteType.labels(dirname.toString, "delete").inc()

                    watchFiles.remove(filename).foreach(_.close())

                    None

                case ENTRY_MODIFY =>
                    fileChangesByteType.labels(dirname.toString, "modified").inc()

                    val logFile = watchFiles.get(filename) match {
                        case Some(log: LogFile) =>
                            log.close()

                            new LogFile(log.file, offset = log.position, checkCrc = checkCrc)

                        case None =>
                            new LogFile(filename.toFile, checkCrc = checkCrc)
                    }

                    watchFiles(filename) = logFile

                    Some(logFile)
            }
        }

        watchKey.reset()

        if (!watchKey.isValid) {
            watchKeys.remove(watchKey)
        }

        changedFiles
    }

    override def getDirectory: String = dir.getAbsolutePath
}
