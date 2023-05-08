package org.apache.spark.sql.msgpack.visitor

import org.apache.spark.sql.msgpack.MessagePackOptions
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ValueVisitorContext(var dataType: DataType = null, val options: MessagePackOptions = MessagePackOptions()) {

  def this(options: MessagePackOptions) = this(null, options)

  private final val paths = ListBuffer[Any]()

  def pathIn(path: Any): ValueVisitorContext = {
    if (options.deserializerTracePath) {
      paths += path
    }
    this
  }

  def pathUpdate(path: Any): ValueVisitorContext = {
    if (options.deserializerTracePath) {
      paths(paths.size - 1) = path
    }
    this
  }

  def pathOut(): ValueVisitorContext = {
    if (options.deserializerTracePath) {
      paths.trimEnd(1)
    }
    this
  }

  def resolvePath: String = {
    val str = new mutable.StringBuilder()
    for ((path, index) <- paths.zipWithIndex) {
      path match {
        case _: Integer =>
          str.append(s"[$path]")
        case _ =>
          if (index > 0) {
            str.append(".")
          }
          str.append(path)
      }
    }
    str.toString
  }

}
