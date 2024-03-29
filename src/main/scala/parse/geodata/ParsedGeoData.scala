package parse.geodata

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object ParsedGeoData {

  def take(k: Int, row: (Int, List[List[List[Double]]])): Double = row._2
    .head
    .drop((k - 1) / 2)
    .head
    .dropRight(k % 2)
    .last
  match {
    case d: Double => d.toDouble
    case _ => 0.0
  }

  val schema =
    StructType(
      StructField("id", IntegerType, false) ::
      StructField("lon1", DoubleType, false) ::
      StructField("lat1", DoubleType, false) ::
      StructField("lon2", DoubleType, false) ::
      StructField("lat2", DoubleType, false) ::
      StructField("lon3", DoubleType, false) ::
      StructField("lat3", DoubleType, false) ::
      StructField("lon4", DoubleType, false) ::
      StructField("lat4", DoubleType, false) :: Nil
  )

  def apply(row: (Int, List[List[List[Double]]])): Row = {

    Row(
      row._1,
      take(1, row),
      take(2, row),
      take(3, row),
      take(4, row),
      take(5, row),
      take(6, row),
      take(7, row),
      take(8, row)
    )
  }

}