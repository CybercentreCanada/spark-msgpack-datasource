package org.apache.spark.sql.msgpack.suite

import com.github.luben.zstd.Zstd
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.test.data.impl.ComplexData
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfter

import java.nio.file.{Files, Paths}

class MessagePackZstdSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  test("read, load and count zst msgpack file") {
    val df = spark.read.format("messagepack").load(new ComplexData().write(compress = true))
    assert(df.count() === 1)
  }

  test("compare both zst and non-zst msgpack file results") {
    val dfMp = spark.read.format("messagepack").load(new ComplexData().write())
    val dfZst = spark.read.format("messagepack").load(new ComplexData().write(compress = true))
    checkAnswer(dfZst, dfMp)
  }

  test("read, load and count a mix bag of zst and non-zst messagepack files in directory") {
    val dataPath = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "mp_data")
    var path = new ComplexData().write("f1.mp", dataPath.toString, compress = false, deleteOnExit = true)
    path = new ComplexData().write("f1.zst", dataPath.toString, compress = true, deleteOnExit = true)
    val zstCount = spark.read.format("messagepack").load(s"${dataPath.toString}/*.zst").count()
    val mpCount = spark.read.format("messagepack").load(s"${dataPath.toString}/*.mp").count()
    val allCount = spark.read.format("messagepack").load(dataPath.toString).count()
    val resultSchema = spark.read.format("messagepack").load(dataPath.toString).schema
    assert(resultSchema === new ComplexData().schema())
    assert(allCount === (zstCount + mpCount))
  }

  test("try out the zstd-jni compress/decompress - ors.") {
    val data = "thisds i26u3s som23ree rand3fom d3rsata izst itzse n23row()?";
    val bytes = data.getBytes
    val compressed = Zstd.compress(bytes);
    val uncompressed = Zstd.decompress(compressed, bytes.length)
    assert(new String(uncompressed) === data)
  }

}
