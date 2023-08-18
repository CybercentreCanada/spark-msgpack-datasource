package org.apache.spark.sql.msgpack.suite

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.msgpack.MessagePackFunctions.from_msgpack
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.msgpack.test.data.impl.{OneRowData, OneRowOneColumnData}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

class MessagePackExpressionsSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.msgpack.MessagePackExtensions")

  val oneRowData = new OneRowData()

  val oneRowOneColumnData = new OneRowOneColumnData()

  var oneRowDf: DataFrame = _

  var oneRowOneColumnDf: DataFrame = _

  protected override def beforeAll(): Unit = {
    // let it do its thing...
    super.beforeAll()

    // setup onerow dataframe.
    var schema = StructType(
      Seq(
        StructField("raw", BinaryType, nullable = true)
      )
    )
    var rdd = spark.sparkContext.parallelize(
      Seq(
        Row(oneRowData.getBytes)
      )
    )
    oneRowDf = spark.createDataFrame(rdd, schema);
    oneRowDf.createTempView("my_table_1")

    schema = StructType(
      Seq(
        StructField("raw", BinaryType, nullable = true)
      )
    )
    rdd = spark.sparkContext.parallelize(
      Seq(
        Row(oneRowOneColumnData.getBytes)
      )
    )
    oneRowOneColumnDf = spark.createDataFrame(rdd, schema);
    oneRowOneColumnDf.createTempView("my_table")
  }

  test("from_msgpack: expression using funciton") {
    val decodedDf = oneRowDf.select(from_msgpack(col("raw"), oneRowData.schema()).alias("decoded"))

    println(oneRowData.schema())
    decodedDf.printSchema()
    oneRowDf.show()
    decodedDf.select("decoded.*").show()
  }

  test("from_msgpack: expression using extension (json schema)") {
    val decodedDf = spark.sql(s"select from_msgpack(raw, '${oneRowData.schema().json}') as decoded from my_table_1")
    decodedDf.printSchema()
    decodedDf.select("decoded.*").show()
  }

  test("from_msgpack: expression using extension (ddl schema)") {
    val decodedDf = spark.sql(s"select from_msgpack(raw, '${oneRowData.schema().toDDL}') as decoded from my_table_1")
    decodedDf.printSchema()
    decodedDf.select("decoded.*").show()
  }
}
