package com.twitter.scalding.parquet.macros

import com.twitter.scalding.parquet.macros.impl.{FieldValuesProvider, ParquetSchemaProvider, ParquetTupleConverterProvider}
import com.twitter.scalding.parquet.tuple.ParquetTupleConverter

import scala.language.experimental.macros

object Macros {
  def caseClassFieldValues[T]: T => Map[Int, Any] = macro FieldValuesProvider.toFieldValuesImpl[T]

  def caseClassParquetSchema[T]: String = macro ParquetSchemaProvider.toParquetSchemaImpl[T]

  def caseClassParquetTupleConverter[T]: ParquetTupleConverter = macro ParquetTupleConverterProvider.toParquetTupleConverterImpl[T]
}
