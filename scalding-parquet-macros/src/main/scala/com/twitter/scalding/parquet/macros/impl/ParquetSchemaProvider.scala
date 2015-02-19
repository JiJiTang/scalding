package com.twitter.scalding.parquet.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl

import scala.reflect.macros.Context

object ParquetSchemaProvider {
  def toParquetSchemaImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[String] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    val REPETITION_REQUIRED = q"_root_.parquet.schema.Type.Repetition.REQUIRED"
    val REPETITION_OPTIONAL = q"_root_.parquet.schema.Type.Repetition.OPTIONAL"

    def createPrimitiveTypeField(isOption: Boolean, primitiveType: Tree, fieldName: String): List[Tree] =
      List(q"""new _root_.parquet.schema.PrimitiveType(${getRepetition(isOption)}, $primitiveType, $fieldName)""")

    def getRepetition(isOption: Boolean): Tree = if (isOption) REPETITION_OPTIONAL else REPETITION_REQUIRED

    def matchField(fieldType: Type, fieldName: String, isOption: Boolean): List[Tree] = {
      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY", fieldName)
        case tpe if tpe =:= typeOf[Boolean] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN", fieldName)
        case tpe if tpe =:= typeOf[Short] || tpe =:= typeOf[Int] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32", fieldName)
        case tpe if tpe =:= typeOf[Long] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64", fieldName)
        case tpe if tpe =:= typeOf[Float] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT", fieldName)
        case tpe if tpe =:= typeOf[Double] =>
          createPrimitiveTypeField(isOption, q"_root_.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE", fieldName)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, fieldName, true)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
          List(q"""new _root_.parquet.schema.GroupType(${getRepetition(isOption)}, $fieldName,
                        _root_.scala.Array.apply[_root_.parquet.schema.Type](..${expandMethod(tpe)}):_*)""")
        case _ => c.abort(c.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type): List[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, fieldName, false)
        }.toList
    }

    val expanded = expandMethod(T.tpe)
    if (expanded.isEmpty)
      c.abort(c.enclosingPosition, s"Case class $T.tpe has no primitive types we were able to extract")
    val messageTypeName = s"${T.tpe}".split("\\.").last
    val schema = q"""new _root_.parquet.schema.MessageType($messageTypeName,
                        _root_.scala.Array.apply[_root_.parquet.schema.Type](..$expanded):_*).toString"""

    c.Expr[String](schema)
  }
}
