package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.msgpack.MessagePackFileFormat
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.v2.msgpack.MessagePackDataSourceV2

class MessagePackSuite extends QueryTest with SharedSparkSession {

  test("resolve 'messagepack' datasource (v1).") {
    val ds = DataSource.lookupDataSource(
      "org.apache.spark.sql.msgpack.MessagePackFileFormat",
      spark.sessionState.conf
    )
    assert(ds === classOf[MessagePackFileFormat])
  }

  test("resolve 'messagepack' datasource (v2).") {
    val ds = DataSource.lookupDataSource(
      "org.apache.spark.sql.v2.msgpack.MessagePackDataSourceV2",
      spark.sessionState.conf
    )
    assert(ds === classOf[MessagePackDataSourceV2])
  }

}
