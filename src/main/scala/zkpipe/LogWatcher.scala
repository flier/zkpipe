package zkpipe

import java.io.{Closeable, File}
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}
import java.nio.file.{FileSystems, Path, WatchKey, WatchService}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.postfixOps

trait Watcher extends Closeable {
    val changes: Stream[LogFile]
}

class LogWatcher(files: Seq[File], checkCrc: Boolean) extends Watcher with LazyLogging{
    val watchFiles: mutable.Map[Path, LogFile] = mutable.Map.empty ++ (files filter { file =>
        file.isFile && file.canRead && new LogFile(file).isValid
    } map { file =>
        (file.toPath, new LogFile(file))
    })

    val watchDirs: Seq[Path] = files map { file =>
        if (file.isDirectory) file else file.getParentFile
    } map { file => file.toPath }

    val watcher: WatchService = FileSystems.getDefault.newWatchService()

    val watchKeys: Map[WatchKey, Path] = watchDirs map { dir =>
        logger.info(s"watching directory `$dir`...")

        (dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY), dir)
    } toMap

    override def close(): Unit = {
        for ( (_, logFile) <- watchFiles ) { logFile.close() }
    }

    override val changes: Stream[LogFile] = {
        def next(): Stream[LogFile] = {
            val watchKey = watcher.take()

            val watchEvents = watchKey.pollEvents().asScala flatMap { watchEvent =>
                val filename = watchKeys(watchKey).resolve(watchEvent.context.asInstanceOf[Path])

                logger.info(s"file `$filename` {}", watchEvent.kind match {
                    case ENTRY_CREATE => "created"
                    case ENTRY_DELETE => "deleted"
                    case ENTRY_MODIFY => "modified"
                })

                watchEvent.kind match {
                    case ENTRY_CREATE =>
                        Some(watchFiles.getOrElseUpdate(filename, new LogFile(filename.toFile, checkCrc = checkCrc)))

                    case ENTRY_DELETE =>
                        watchFiles.remove(filename).foreach(_.close())

                        None

                    case ENTRY_MODIFY =>
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

            Stream.concat(watchEvents) #::: next()
        }

        next()
    }
}
