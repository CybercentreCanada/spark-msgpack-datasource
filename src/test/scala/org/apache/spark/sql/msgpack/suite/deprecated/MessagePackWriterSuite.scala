package org.apache.spark.sql.msgpack.suite.deprecated

import org.apache.spark.sql.msgpack.test.utils.MessagePackSuiteUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row}

import java.sql.Date
import java.util.TimeZone

class MessagePackWriterSuite extends QueryTest with SharedSparkSession {

  test("test writting data into messagepack format.") {
    val schema = StructType(
      Seq(
        StructField("int", IntegerType, nullable = true),
        StructField("long", LongType, nullable = true),
        StructField("float", FloatType, nullable = true),
        StructField("byte", ByteType, nullable = true),
        StructField("boolean", BooleanType, nullable = true)
      )
    )

    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(1, 1.toLong, 1.toFloat, 1.toByte, true),
        Row(2, 2.toLong, 2.toFloat, 2.toByte, true),
        Row(3, 3.toLong, 3.toFloat, 3.toByte, true),
        Row(4, 4.toLong, 99.toFloat, 4.toByte, true)
      )
    )

    val df = spark.createDataFrame(rdd, schema);

    withTempPath { dir =>
      df.write.format("messagepack").save(dir.toString)
      val wdf = spark.read.format("messagepack").load(dir.toString)
      assert(wdf.count() === rdd.count())
    }
  }

  test("test DateType field.") {
    // set up timezone in order ensure it lines up with base epoc.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    //
    val ts1 = MessagePackSuiteUtil.timestamp(2020, 3, 27)
    val ts2 = MessagePackSuiteUtil.timestamp(2020, 3, 26)
    val d1 = new Date(ts1.getTime)
    val d2 = new Date(ts2.getTime)

    val schema = StructType(
      Seq(
        StructField("date", DateType, nullable = true)
      )
    )

    // DateType only goes up to days.
    // This will remove hours/min/sec/etc... information.
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(d1),
        Row(d2)
      )
    )
    val df = spark.createDataFrame(rdd, schema);

    withTempPath { dir =>
      // write it.
      // Here the date objects are saved into timestamp extenstion types.
      df.write.format("messagepack").save(dir.toString)

      // read it back.
      // Date objects are now Timestamps.
      val rdf = spark.read.format("messagepack").load(dir.toString)

      //
      assert(rdf.count() === rdd.count())

      // Ensure that is the case.
      // Verify that date object are indeed timestamps now.
      checkAnswer(
        rdf.select("date"),
        Seq(Row(ts1), Row(ts2))
      )
    }
  }

  test("test TimestampType field") {
    // set up timezone in order ensure it lines up with base epoc.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    //
    val ts1 = MessagePackSuiteUtil.now(111111111)
    val ts2 = MessagePackSuiteUtil.now(111111111)

    val schema = StructType(
      Seq(
        StructField("ts", TimestampType, nullable = true)
      )
    )

    // Spark timestamp will only to up to micosecs it seems, this will remove
    //   last three digits of nanos.
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(ts1),
        Row(ts2)
      )
    )
    val df = spark.createDataFrame(rdd, schema);

    // save it and read it back.
    withTempPath { dir =>
      // write it.
      df.write.format("messagepack").save(dir.toString)

      // read it back.
      val rdf = spark.read.format("messagepack").load(dir.toString)

      //
      assert(rdf.count() === rdd.count())

      //
      checkAnswer(
        df.select("ts"),
        rdf.select("ts")
      )
    }
  }

  test("test DecimalType field") {
    val schema = StructType(
      Seq(
        StructField("decimal", DecimalType(12, 2), nullable = true)
      )
    )

    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(BigDecimal(9654978880.10))
      )
    )

    val df = spark.createDataFrame(rdd, schema);

    withTempPath { dir =>
      //
      df.write.format("messagepack").save(dir.toString)

      // select with schema to check if it used a decimaltype
      //  and now a string.
      val rdf =
        spark.read.format("messagepack").schema(schema).load(dir.toString)

      checkAnswer(
        df.select("decimal"),
        rdf.select("decimal")
      )
    }
  }

  test(
    "test StringType/IntegerType/FloatType/BooleanType/ByteType/NullType field"
  ) {

    val schema = StructType(
      Seq(
        StructField("string", StringType, nullable = true),
        StructField("integer", IntegerType, nullable = true),
        StructField("long", LongType, nullable = true),
        StructField("double", DoubleType, nullable = true),
        StructField("float", FloatType, nullable = true),
        StructField("boolean", BooleanType, nullable = true),
        StructField("byte", ByteType, nullable = true),
        StructField("null", NullType, nullable = true)
      )
    )

    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(
          "string1",
          11,
          12.toLong,
          13.toDouble,
          14.toFloat,
          false,
          1.toByte,
          null
        ),
        Row(
          "string2",
          21,
          22.toLong,
          23.toDouble,
          22.toFloat,
          true,
          2.toByte,
          null
        ),
        Row(
          "string3",
          1234,
          4034556529L,
          4034556529L.toDouble,
          1234.toFloat,
          true,
          12.toByte,
          null
        ),
        Row(
          "string4",
          1234345,
          40345565223459L,
          40345565223459L.toDouble,
          1234.toFloat,
          true,
          12.toByte,
          null
        ),
        Row(
          "string5",
          Integer.MAX_VALUE,
          Long.MaxValue,
          Double.MaxValue,
          Float.MaxValue,
          true,
          Byte.MaxValue,
          null
        ),
        Row(
          "string6",
          Integer.MIN_VALUE,
          Long.MinValue,
          Double.MinValue,
          Float.MinValue,
          true,
          Byte.MinValue,
          null
        )
      )
    )

    val df = spark.createDataFrame(rdd, schema);

    withTempPath { dir =>
      //
      df.write.format("messagepack").save(dir.toString)

      // use the same schema to assert that it uses this ordering, and not
      //  the schema inference's natural sort order.
      val rdf =
        spark.read.format("messagepack").schema(schema).load(dir.toString)

      //
      checkAnswer(
        df.select("*"),
        rdf.select("*")
      )
    }
  }

  // TODO:
  test("test ArrayType field") {
    // schema.
    val schema = StructType(
      Seq(
        StructField("string_array", ArrayType(StringType), nullable = false),
        StructField("short_array", ArrayType(ShortType), nullable = false),
        StructField("integer_array", ArrayType(IntegerType), nullable = false),
        StructField("long_array", ArrayType(LongType), nullable = false),
        StructField("double_array", ArrayType(DoubleType), nullable = false),
        StructField("float_array", ArrayType(FloatType), nullable = false),
        StructField(
          "decimal_array",
          ArrayType(DecimalType(10, 5)),
          nullable = false
        )
      )
    )

    // data.
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(
          Array[String]("string1", "string2", "string3"),
          Array[Short](1, 2, 3, 4),
          Array[Int](1, 2, 3, 4),
          Array[Long](1L, 2L, 3L, 4L),
          Array[Double](1.0, 2.0, 3.0, 4.0),
          Array[Float](1f, 2f, 3f, 4f),
          Array[BigDecimal](
            BigDecimal(1111100001, 5),
            BigDecimal(1111100002, 5),
            BigDecimal(1111100003, 5),
            BigDecimal(1111100004, 5),
            BigDecimal(1111100005, 5)
          )
        )
      )
    )

    // dataframe.
    val df = spark.createDataFrame(rdd, schema)

    // write it and assert we haven't lost anything.
    withTempPath { dir =>
      df.write.format("messagepack").save(dir.toString)

      val rdf =
        spark.read.format("messagepack").schema(schema).load(dir.toString)

      assert(rdf.count() === df.count())
      checkAnswer(
        df.select("*"),
        rdf.select("*")
      )
    }
  }

  // TODO:
  test("test StructType field") {}

  // TODO:
  test("test BinaryType field") {}

}
