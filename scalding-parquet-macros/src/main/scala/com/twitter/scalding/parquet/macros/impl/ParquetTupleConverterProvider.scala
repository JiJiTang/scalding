package com.twitter.scalding.parquet.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.parquet.tuple.ParquetTupleConverter

import scala.reflect.macros.Context

object ParquetTupleConverterProvider {
  def toParquetTupleConverterImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[ParquetTupleConverter] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def buildGroupConverter(tpe: Type, parentTree: Tree, isOption: Boolean, idx: Int, converterBodyTree: Tree): Tree = {
      q"""new _root_.com.twitter.scalding.parquet.tuple.ParquetTupleConverter($parentTree, $isOption, $idx){
            override def newConverter(i: Int): _root_.parquet.io.api.Converter = {
              $converterBodyTree
              throw new RuntimeException("invalid index: " + i)
            }

            override def createValue(): Any = {
              if(fieldValues.isEmpty) null
              else classOf[$tpe].getConstructors()(0).newInstance(fieldValues.toSeq.map(_.asInstanceOf[AnyRef]): _*)
            }
          }"""
    }

    def matchField(idx: Int, fieldType: Type, isOption: Boolean): List[Tree] = {

      def createConverter(idx: Int, converter: Tree): Tree = q"if($idx == i) return $converter"

      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.StringConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Boolean] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.BooleanConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Short] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.ShortConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Int] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.IntConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Long] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.LongConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Float] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.FloatConverter($idx, this, $isOption)"))
        case tpe if tpe =:= typeOf[Double] =>
          List(createConverter(idx, q"new _root_.com.twitter.scalding.parquet.tuple.DoubleConverter($idx, this, $isOption)"))
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(idx, innerType, true)
        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) =>
          val innerConverterTrees = buildConverterBody(tpe, expandMethod(tpe))
          List(createConverter(idx, buildGroupConverter(tpe, q"Option(this)", isOption, idx, innerConverterTrees)))
        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type): List[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .zipWithIndex
        .flatMap {
          case (accessorMethod, idx) =>
            val fieldType = accessorMethod.returnType
            matchField(idx, fieldType, false)
        }.toList
    }

    def buildConverterBody(tpe: Type, trees: List[Tree]): Tree = {
      if (trees.isEmpty)
        ctx.abort(ctx.enclosingPosition, s"Case class $tpe has no primitive types we were able to extract")
      trees.foldLeft(q"") {
        case (existingTree, t) =>
          q"""$existingTree
              $t"""
      }
    }

    val groupConverter = buildGroupConverter(T.tpe, q"None", false, -1, buildConverterBody(T.tpe, expandMethod(T.tpe)))

    ctx.Expr[ParquetTupleConverter](q"""
       $groupConverter
     """)
  }
}
