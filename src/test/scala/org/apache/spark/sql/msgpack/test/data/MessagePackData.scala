package org.apache.spark.sql.msgpack.test.data

import com.github.luben.zstd.Zstd
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.msgpack.core.{MessageBufferPacker, MessagePack}

import java.nio.file.{Files, Path, Paths}

abstract class MessagePackData {

  private var filePath: Path = _

  protected val packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()

  def pack(): Unit

  def schema(): StructType = throw new UnsupportedOperationException()

  def name(): String = {
    getClass.getSimpleName
  }

  def getBytes: Array[Byte] = {
    pack()
    packer.toByteArray
  }

  def write(compress: Boolean = false, deleteOnExit: Boolean = true): String = {
    val _filePath = {
      Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "_spark_messagepack_data")
    }
    if (deleteOnExit) {
      _filePath.toFile.deleteOnExit()
    }
    write(_filePath.toString, compress, deleteOnExit)
  }

  def write(directory: String, compress: Boolean, deleteOnExit: Boolean): String = {
    write(s"${name()}.${if (compress) "zst" else "mp"}", directory, compress, deleteOnExit)
  }

  def write(name: String, directory: String, compress: Boolean, deleteOnExit: Boolean): String = {
    val path = Paths.get(directory, name)
    val content = if (compress) Zstd.compress(getBytes) else getBytes

    if (!path.toFile.getParentFile.exists()) {
      path.toFile.getParentFile.createNewFile()
    }

    filePath = Files.write(path, content)
    if (deleteOnExit) {
      filePath.toFile.deleteOnExit()
    }
    filePath.toAbsolutePath.toString
  }

  def delete(): Unit = {
    Files.delete(filePath)
  }

  def onFs: Boolean = {
    Files.exists(filePath)
  }

}
