/*
 Copyright 2014 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.twitter.scalding.commons.macros.impl.ordser

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scrooge.{ ThriftUnion, ThriftStruct }
import com.twitter.scalding.macros.impl.ordser._

object ScroogeOuterOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftStruct] && !(tpe <:< typeOf[ThriftUnion]) => ScroogeOuterOrderedBuf(c)(tpe)
    }
    pf
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val variableID = (outerType.typeSymbol.fullName.hashCode.toLong + Int.MaxValue.toLong).toString
    val variableNameStr = s"bufferable_$variableID"
    val variableName = newTermName(variableNameStr)
    val implicitInstanciator = q"""implicitly[_root_.com.twitter.scalding.serialization.OrderedSerialization[$outerType]]"""

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        q"$variableName.compareBinary($inputStreamA, $inputStreamB).unsafeToInt"
      override def hash(element: ctx.TermName): ctx.Tree = q"$variableName.hash($element)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"$variableName.write($inputStream, $element)"

      override def length(element: Tree) =
        MaybeLengthCalculation(c)(q"""
          $variableName.staticSize match {
            case Some(s) => Option(Left(s)): Option[Either[Int, Int]]
            case None =>
              $variableName.dynamicSize($element) match {
                case Some(s) =>
                  Option(Right(s)) : Option[Either[Int, Int]]
                case None => None
              }
          }
          """)

      override def get(inputStream: ctx.TermName): ctx.Tree =
        q"$variableName.read($inputStream).get"

      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        q"$variableName.compare($elementA, $elementB)"
      override val lazyOuterVariables = Map(variableNameStr -> implicitInstanciator)

    }
  }
}
