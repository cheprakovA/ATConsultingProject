package parse.geodata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import parse.communication.CommunicationDataParser

import scala.util.parsing.json.JSON

object GEODataParser {

  def createData(c: Char): List[Row] = {

    CommunicationDataParser.setIndexes()

    val config = new Config()

    val textFile: RDD[String] = config.sc.textFile(config.path)
    val jsonStr = textFile.collect().mkString("")

    config.sc.stop()

    var tempGeoData = for {
      Some(M(map)) <- List(JSON.parseFull(jsonStr))
      M(crs) = map("crs")
      S(_) = crs("type")
      M(properties) = crs("properties")
      S(_) = properties("name")
      S(_) = map("type")
      L(features) = map("features")
      M(feature) <- features
      M(geometry) = feature("geometry")
      S(_) = geometry("type")
      LLLD(coordinates) = geometry("coordinates")
      S(_) = feature("type")
      D(_) = feature("id")
      M(feature_properties) = feature("properties")
      D(cell_id) = feature_properties("cellId")
    } yield (cell_id.toInt, coordinates)

    c match {
      case 'w' =>
        tempGeoData = tempGeoData.filter(tgd => CommunicationDataParser.workZonesIndexes.contains(tgd._1))
      case 'r' =>
        tempGeoData = tempGeoData.filter(tgd => CommunicationDataParser.relaxZonesIndexes.contains(tgd._1))
      case 'o' =>
        tempGeoData = tempGeoData.filter(tgd => CommunicationDataParser.otherZonesIndexes.contains(tgd._1))
    }

    tempGeoData.map(t => ParsedGeoData(t))
  }

}