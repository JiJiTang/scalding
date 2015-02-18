package com.twitter.scalding.parquet.tuple.macros.impl

import scala.language.experimental.macros
import parquet.schema.MessageType

import scala.reflect.macros.Context

object SchemaProviderImpl {

  class ParquetSchemaCaseClassVisitor[TREE](val c: Context) extends CaseClassVisitor[TREE] {
    import c.universe._

    case class Extractor(tpe: Type, toTree: Tree)
    case class Builder(toTree: Tree = q"")

    implicit val builderLiftable = new Liftable[Builder] {
      def apply(b: Builder): Tree = b.toTree
    }

    implicit val extractorLiftable = new Liftable[Extractor] {
      def apply(b: Extractor): Tree = b.toTree
    }
    
    val REPETITION_REQUIRED = q"ParquetType.Repetition.REQUIRED"
    val REPETITION_OPTIONAL = q"ParquetType.Repetition.OPTIONAL"

    def getRepetition(isOption: Boolean): TREE = if (isOption) REPETITION_OPTIONAL else REPETITION_REQUIRED

    def createPrimitiveTypeField(isOption: Boolean, primitiveType: String, fieldName: String): List[TREE] =
      List(q"""new PrimitiveType(${getRepetition(isOption)}, q"$primitiveType", $fieldName)""")

    override def visitInt(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.INT32", s"$outerName$fieldName")

    override def visitDouble(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.DOUBLE", s"$outerName$fieldName")

    override def visitLong(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.INT64", s"$outerName$fieldName")

    override def visitBoolean(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.BOOLEAN", s"$outerName$fieldName")

    override def visitFloat(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.FLOAT", s"$outerName$fieldName")

    override def visitShort(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.INT32", s"$outerName$fieldName")

    override def visitString(outerName: String, fieldName: String, optional: Boolean): List[TREE] =
      createPrimitiveTypeField(optional, "PrimitiveType.PrimitiveTypeName.BINARY", s"$outerName$fieldName")
  }

  def toParquetSchemaImp[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[MessageType] = {
    import c.universe._
    val schemaVisitor = new ParquetSchemaCaseClassVisitor[c.universe.Tree](c)
    val trees = new CaseClassVisitable[T].accept[c.universe.Tree](c)(schemaVisitor)(T)
    val messageTypeName = s"${T.tpe}".split("\\.").last
    c.Expr[MessageType](q"""import parquet.schema.{MessageType, PrimitiveType, Type => ParquetType}
          new MessageType($messageTypeName, Array.apply[ParquetType](..$trees):_*)""")
  }
}
