package com.twitter.scalding.parquet.tuple

import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scalding.parquet.HasFilterPredicate
import com.twitter.scalding.parquet.tuple.scheme.{ParquetWriteSupport, ParquetReadSupport, TypedParquetTupleScheme}

import scala.reflect.ClassTag

/**
 * Typed parquet tuple source/sink
 * @author Jian Tang
 */
object TypedParquetTuple {
  /** 
   * Create readable typed parquet source.
   * Here is an example: 
   *
   *  case class SampleClassB(string: String, int: Int, double: Option[Double], a: SampleClassA)
   *
   *  class ReadSupport extends ParquetReadSupport[SampleClassB] {
   *    override val tupleConverter: ParquetTupleConverter = Macros.caseClassParquetTupleConverter[SampleClassB]
   *    override val rootSchema: String = Macros.caseClassParquetSchema[SampleClassB]
   *  }
   *
   *  val parquetTuple = TypedParquetTuple[SampleClassB, ReadSupport](Seq(outputPath)) 
   *
   * @param paths paths of parquet I/O
   * @param t Read support type tag
   * @tparam T Tuple type
   * @tparam R Read support type
   * @return a typed parquet source.
   */
  def apply[T, R <: ParquetReadSupport[T]](paths: Seq[String])(implicit t: ClassTag[R]) =
    new TypedFixedPathParquetTuple[T, R, ParquetWriteSupport[T]](paths, t.runtimeClass.asInstanceOf[Class[R]], null)

  /**
   * Create sinkable typed parquet source.
   * Here is an example: 
   *  
   *  case class SampleClassB(string: String, int: Int, double: Option[Double], a: SampleClassA)
   *  
   *  class WriteSupport extends ParquetWriteSupport[SampleClassB] {
   *    override val fieldValues: (SampleClassB) => Map[Int, Any] = Macros.caseClassFieldValues[SampleClassB]
   *    override val rootSchema: String = Macros.caseClassParquetSchema[SampleClassB]
   *  }
   *    
   *  val parquetTuple = TypedParquetTuple[SampleClassB, WriteSupport](Seq(outputPath))
   *
   * @param paths paths of parquet I/O
   * @param t Read support type tag
   * @tparam T Tuple type
   * @tparam W Write support type
   * @return a typed parquet source.
   */
  def apply[T, W <: ParquetWriteSupport[T]](paths: Seq[String])(implicit t: ClassTag[W]) =
    new TypedFixedPathParquetTuple[T, ParquetReadSupport[T], W](paths, null, t.runtimeClass.asInstanceOf[Class[W]])

  /**
   * Create typed parquet source supports both R/W.
   * @param paths paths of  parquet I/O
   * @param r Read support type tag
   * @param w Write support type tag
   * @tparam T Tuple type
   * @tparam R Read support type
   * @return a typed parquet source.
   */
  def apply[T, R <: ParquetReadSupport[T], W <: ParquetWriteSupport[T]](paths: Seq[String])(implicit r: ClassTag[R], 
        w: ClassTag[W]) = {
    val readSupport = r.runtimeClass.asInstanceOf[Class[R]]
    val writeSupport = w.runtimeClass.asInstanceOf[Class[W]]
    new TypedFixedPathParquetTuple[T, R, W](paths, readSupport, writeSupport)
  }

}

/**
 * Typed Parquet tuple source/sink.
 */
trait TypedParquetTuple[T, R <: ParquetReadSupport[T], W <: ParquetWriteSupport[T]] extends FileSource
  with Mappable[T] with TypedSink[T] with HasFilterPredicate {

  val readSupport: Class[R]
  val writeSupport: Class[W]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  override def hdfsScheme = {
    val scheme = new TypedParquetTupleScheme[T](readSupport, writeSupport, withFilter)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

class TypedFixedPathParquetTuple[T, R <: ParquetReadSupport[T], W <: ParquetWriteSupport[T]](val paths: Seq[String],
  val readSupport: Class[R], val writeSupport: Class[W]) extends FixedPathSource(paths: _*) with TypedParquetTuple[T, R, W]
