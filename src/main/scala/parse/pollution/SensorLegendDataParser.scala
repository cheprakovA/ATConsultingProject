package parse.pollution

import app.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object SensorLegendDataParser extends App{

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("appName")
    .getOrCreate()

  import spark.implicits._

  var anotherOne = Seq.empty[(Int, Double)]

  val workZonesDF = spark
    .read
    .parquet(Constants.OUTPUT_PATH +
      Constants.ZONES_COORDS_PATH + Constants.WORKING_ZONES_PATH)

  workZonesDF.createOrReplaceTempView("work_zones")

  val relaxZonesDF = spark
    .read
    .parquet(Constants.OUTPUT_PATH +
      Constants.ZONES_COORDS_PATH + Constants.RELAX_ZONES_PATH)

  val otherZonesDF = spark
    .read
    .parquet(Constants.OUTPUT_PATH +
      Constants.ZONES_COORDS_PATH + Constants.OTHER_ZONES_PATH)

  val legendDF = spark.read
    .format(Constants.CSV)
    .load(Constants.SENSOR_LEGEND_PATH)
    .select($"_c0" as "sensor_id", $"_c1" as "street", $"_c2" as "latitude",
      $"_c3" as "longitude", $"_c4" as "type", $"_c5" as "uom", $"_c6" as "time_int")
    .withColumn("sensor_id", col("sensor_id").cast(IntegerType))
    .withColumn("latitude", col("latitude").cast(DoubleType))
    .withColumn("longitude", col("longitude").cast(DoubleType))

  legendDF.createOrReplaceTempView("legend")

  val reader = spark.read

  val tmpVal =
    legendDF.select("sensor_id").as[Int].collect().map(
      i => reader
        .format(Constants.CSV)
        .load(s"/Volumes/DATA/POLLUTION/SENSOR-VALUES/mi_pollution_$i.csv")
        .select($"_c2" as "val", $"_c0" as "sens_id")
        .withColumn("val", col("val").cast(DoubleType))
        .withColumn("sens_id", col("sens_id").cast(IntegerType))
        .withColumn("count", lit(1.0).cast(DoubleType))
    )

  tmpVal.foreach(
    df => df.groupBy("sens_id").sum("val", "count").foreach(
      row => {
        anotherOne = anotherOne :+
          (row.getAs[Int](0), row.getAs[Double](1) / row.getAs[Double](2))
      }
    )
  )

  val rdd = spark.sparkContext.parallelize(anotherOne)
    .toDF("sens_id", "value")
    .createOrReplaceTempView("tempView")

  val temp =
    spark.sql("select id, sensor_id, street, type, uom from work_zones " +
      "inner join legend on legend.longitude > work_zones.lon1 and legend.longitude < work_zones.lon3 and " +
      "legend.latitude > work_zones.lat3 and legend.longitude < work_zones.lat1")
    .createOrReplaceTempView("tempe")

  val _temp =
    spark.sql("select * from tempe inner join tempView on " +
      "tempe.sensor_id = tempView.sens_id")
    .drop("sens_id").show(100)

}
