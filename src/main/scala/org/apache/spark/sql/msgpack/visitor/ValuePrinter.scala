package org.apache.spark.sql.msgpack.visitor

import org.apache.spark.sql.msgpack.MessagePackOptions
import org.msgpack.core.MessagePack
import org.msgpack.value._

import java.nio.file.{Files, Paths}

object ValuePrinter extends ValueVisitor {

  def onFile(path: String): Unit = {
    val unpacker = MessagePack.newDefaultUnpacker(Files.newInputStream(Paths.get(path)))
    var rootIndex = 0
    while (unpacker.hasNext) {
      val context = new ValueVisitorContext(MessagePackOptions.builder.setTracePath(true).get)
      context.pathIn(s"root[$rootIndex]")
      visit(unpacker.unpackValue(), context)
      context.pathOut()
      rootIndex += 1
    }
  }

  override protected def visit(value: MapValue, context: ValueVisitorContext): Any = {
    val iterator = value.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      val key = entry.getKey
      val value = entry.getValue
      context.pathIn(key)
      visit(value, context)
      context.pathOut()
    }
  }

  override protected def visit(value: ArrayValue, context: ValueVisitorContext): Any = {
    val iterator = value.iterator()
    context.pathIn(-1)
    var index = 0
    while (iterator.hasNext) {
      context.pathUpdate(index)
      val value = iterator.next
      visit(value, context)
      index += 1
    }
    context.pathOut()
  }

  override protected def visit(value: StringValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: IntegerValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: FloatValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: BooleanValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: BinaryValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: NilValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }

  override protected def visit(value: ExtensionValue, context: ValueVisitorContext): Any = {
    println(s"${context.resolvePath} = $value")
  }
}
