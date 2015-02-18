package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import scala.language.experimental.macros
import scala.reflect.macros.Context

class CaseClassVisitable[T] {
  def accept[TREE](c: Context)(visitor: CaseClassVisitor[TREE])(implicit T: c.WeakTypeTag[T]): List[TREE] = {
    import c.universe._

    def matchField(fieldType: Type, outerName: String, fieldName: String, isOption: Boolean): List[TREE] = {
      val parquetFieldName = s"$outerName$fieldName"
      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          visitor.visitString(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Boolean] =>
          visitor.visitBoolean(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Short] =>
          visitor.visitShort(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Int] =>
          visitor.visitInt(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Long] =>
          visitor.visitLong(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Float] =>
          visitor.visitFloat(outerName, fieldName, isOption)
        case tpe if tpe =:= typeOf[Double] =>
          visitor.visitDouble(outerName, fieldName, isOption)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && isOption =>
          c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, outerName, fieldName, isOption = true)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, s"$parquetFieldName.", isOption = false)
        case _ => c.abort(c.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, outerName: String, isOption: Boolean): List[TREE] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, outerName, fieldName, isOption = false)
        }.toList
    }

    def expandCaseClass(outerTpe: Type, outerName: String, isOption: Boolean): List[TREE] = {
      val expanded = expandMethod(outerTpe, outerName, isOption)
      if (expanded.isEmpty) c.abort(c.enclosingPosition, s"Case class $outerTpe has no primitive types we were able to extract")
      expanded
    }

    expandCaseClass(T.tpe, "", isOption = false)
  }
}

trait CaseClassVisitor[TREE] {

  def visitInt(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitShort(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitLong(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitFloat(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitDouble(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitBoolean(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]

  def visitString(outerName: String, fieldName: String, optional: Boolean = false): List[TREE]
}