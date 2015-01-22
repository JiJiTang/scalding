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
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.macros.impl.ordser._

object UnionLike {

  def compareBinary(c: Context)(inputStreamA: c.TermName, inputStreamB: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))
    val valueA = freshT("valueA")
    val valueB = freshT("valueB")
    val idxCmp = freshT("idxCmp")

    val compareSameTypes: Tree = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>

        val commonCmp: Tree = optiTBuf.map{ tBuf =>
          tBuf.compareBinary(inputStreamA, inputStreamB)
        }.getOrElse[Tree](q"0")

        existing match {
          case Some(t) =>
            Some(q"""
              if($valueA == $idx) {
                $commonCmp
              } else {
                $t
              }
            """)
          case None =>
            Some(q"""
                if($valueA == $idx) {
                  $commonCmp
                } else {
                  sys.error("Unable to compare unknown type")
                }""")
        }
    }.get

    q"""
        val $valueA: Int = $inputStreamA.readByte.toInt
        val $valueB: Int = $inputStreamB.readByte.toInt
        val $idxCmp: Int = $valueA.compare($valueB)
        if($idxCmp != 0) {
          $idxCmp
        } else {
          $compareSameTypes
        }
      """
  }

  def put(c: Context)(inputStream: c.TermName, element: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val innerArg = freshT("innerArg")
    subData.foldLeft(Option.empty[Tree]) {
      case (optiExisting, (idx, tpe, optiTBuf)) =>
        val commonPut: Tree = optiTBuf.map { tBuf =>
          q"""val $innerArg: $tpe = $element.asInstanceOf[$tpe]
              ${tBuf.put(inputStream, innerArg)}
              """
        }.getOrElse[Tree](q"()")

        optiExisting match {
          case Some(s) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $inputStream.writeByte($idx.toByte)
              $commonPut
            } else {
              $s
            }
            """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $inputStream.writeByte($idx.toByte)
              $commonPut
            }
            """)
        }
    }.get
  }

  def length(c: Context)(element: c.Tree)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): LengthTypes[c.type] = {
    import c.universe._

    val prevSizeData = subData.foldLeft(Option.empty[Tree]) {
      case (optiTree, (idx, tpe, tBufOpt)) =>

        val baseLenT: Tree = tBufOpt.map{ tBuf =>
          tBuf.length(q"$element.asInstanceOf[$tpe]") match {
            case m: MaybeLengthCalculation[_] =>
              m.asInstanceOf[MaybeLengthCalculation[c.type]].t

            case f: FastLengthCalculation[_] =>
              q"""Some(Right(${f.asInstanceOf[FastLengthCalculation[c.type]].t})) : Option[Either[Int, Int]]"""

            case _: NoLengthCalculationAvailable[_] =>
              return NoLengthCalculationAvailable(c)
            case e => sys.error("unexpected input to union length code of " + e)
          }
        }.getOrElse(q"Some[Either[Int, Int]](Right(1))")

        val lenT = q"""
        $baseLenT.map{ either =>
        either match {
                case(Left(l)) => Right(l + 1) : Either[Int, Int]
                case(Right(r)) => Right(r + 1) : Either[Int, Int]
              }
            }
              """
        optiTree match {
          case Some(t) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $lenT
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
            $lenT
          } else {
            sys.error("Did not understand thrift union type")
            }""")
        }
    }.get

    MaybeLengthCalculation(c) (prevSizeData)
  }

  def get(c: Context)(inputStream: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val valueA = freshT("valueA")

    val expandedOut = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>
        val extract = optiTBuf.map { tBuf =>
          q"""
            ${tBuf.get(inputStream)}
          """
        }.getOrElse {
          q"""(new Object).asInstanceOf[$tpe]"""
        }

        existing match {
          case Some(t) =>
            Some(q"""
            if($valueA == $idx) {
              $extract : $tpe
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
          if($valueA == $idx) {
            $extract
          } else {
            sys.error("Did not understand thrift union idx: " + $valueA)
          }
            """)
        }
    }.get

    q"""
        val $valueA: Int = $inputStream.readByte.toInt
        $expandedOut
      """
  }

  def compare(c: Context)(cmpType: c.Type, elementA: c.TermName, elementB: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._

    def freshT(id: String) = newTermName(c.fresh(id))

    val arg = freshT("arg")
    val idxCmp = freshT("idxCmp")
    val idxA = freshT("idxA")
    val idxB = freshT("idxB")

    val toIdOpt: Tree = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, _)) =>
        existing match {
          case Some(t) =>
            Some(q"""
            if($arg.isInstanceOf[$tpe]) {
              $idx
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
              if($arg.isInstanceOf[$tpe]) {
                $idx
              } else {
                sys.error("Unable to compare unknown type")
              }""")
        }
    }.get

    val compareSameTypes: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>
        val commonCmp = optiTBuf.map { tBuf =>
          val aTerm = freshT("aTerm")
          val bTerm = freshT("bTerm")
          q"""
          val $aTerm: $tpe = $elementA.asInstanceOf[$tpe]
          val $bTerm: $tpe = $elementB.asInstanceOf[$tpe]
          ${tBuf.compare(aTerm, bTerm)}
        """
        }.getOrElse(q"0")

        existing match {
          case Some(t) =>
            Some(q"""
            if($idxA == $idx) {
              $commonCmp : Int
            } else {
              $t : Int
            }
          """)
          case None =>
            Some(q"""
              if($idxA == $idx) {
                $commonCmp : Int
              } else {
                $idxCmp
              }""")
        }
    }

    val compareFn = q"""
      def instanceToIdx($arg: $cmpType): Int = {
        ${toIdOpt}: Int
      }

      val $idxA: Int = instanceToIdx($elementA)
      val $idxB: Int = instanceToIdx($elementB)
      val $idxCmp: Int = $idxA.compare($idxB)

      if($idxCmp != 0) {
        $idxCmp: Int
      } else {
        ${compareSameTypes.get}: Int
      }
    """

    compareFn
  }
}
