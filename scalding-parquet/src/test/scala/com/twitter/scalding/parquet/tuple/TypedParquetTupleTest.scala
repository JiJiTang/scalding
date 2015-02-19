package com.twitter.scalding.parquet.tuple

import java.io.File

import com.twitter.scalding.parquet.macros.Macros
import com.twitter.scalding.parquet.tuple.scheme.{ParquetReadSupport, ParquetWriteSupport}
import com.twitter.scalding.platform.{HadoopPlatformJobTest, HadoopPlatformTest}
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Args, Job, TypedTsv}
import org.scalatest.{Matchers, WordSpec}

class TypedParquetTupleTest extends WordSpec with Matchers with HadoopPlatformTest {
  "TypedParquetTuple" should {

    "read and write correctly" in {
      import com.twitter.scalding.parquet.tuple.TestValues._
      val tempParquet = java.nio.file.Files.createTempDirectory("parquet_tuple_test_parquet_").toAbsolutePath.toString
      try {
        HadoopPlatformJobTest(new WriteToTypedParquetTupleJob(_), cluster)
          .arg("output", tempParquet)
          .run

        HadoopPlatformJobTest(new ReadFromTypedParquetTupleJob(_), cluster)
          .arg("input", tempParquet)
          .sink[Float]("output") { _.toSet shouldBe values.map(_.a.float).toSet }
          .run
      } finally {
        deletePath(tempParquet)
      }
    }
  }

  def deletePath(path: String) = {
    val dir = new File(path)
    for {
      files <- Option(dir.listFiles)
      file <- files
    } file.delete()
    dir.delete()
  }
}

object TestValues {
  val values = Seq(SampleClassB("B1", 1, Some(4.0D), SampleClassA(bool = true, 5, 1L, 1.2F)),
                   SampleClassB("B2", 2, Some(3.0D), SampleClassA(bool = false,4, 2L, 2.3F)),
                   SampleClassB("B3", 2, None, SampleClassA(bool = true, 6, 3L, 3.4F)),
                   SampleClassB("B4", 2, Some(5.0D), SampleClassA(bool = false, 7, 4L, 4.5F)))
}

case class SampleClassA(bool: Boolean, short: Short, long: Long, float: Float)
case class SampleClassB(string: String, int: Int, double: Option[Double], a: SampleClassA)


class ReadSupport extends ParquetReadSupport[SampleClassB] {
  override val tupleConverter: ParquetTupleConverter = Macros.caseClassParquetTupleConverter[SampleClassB]
  override val rootSchema: String = Macros.caseClassParquetSchema[SampleClassB]
}

class WriteSupport extends ParquetWriteSupport[SampleClassB] {
  override val fieldValues: (SampleClassB) => Map[Int, Any] = Macros.caseClassFieldValues[SampleClassB]
  override val rootSchema: String = Macros.caseClassParquetSchema[SampleClassB]
}

/**
 * Test job write a sequence of sample class values into a typed parquet tuple.
 * To test typed parquet tuple can be used as sink
 */
class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
  import com.twitter.scalding.parquet.tuple.TestValues._

  val outputPath = args.required("output")

  val parquetTuple = TypedParquetTuple[SampleClassB, WriteSupport](Seq(outputPath))
  TypedPipe.from(values).write(parquetTuple)
}

/**
 * Test job read from a typed parquet tuple and write the mapped value into a typed csv sink
 * To test typed parquet tuple can bse used as source and read data correctly
 */
class ReadFromTypedParquetTupleJob(args: Args) extends Job(args) {
  
  val inputPath = args.required("input")
  
  val parquetTuple = TypedParquetTuple[SampleClassB, ReadSupport](Seq(inputPath))

  TypedPipe.from(parquetTuple).map(_.a.float).write(TypedTsv[Float]("output"))
}
